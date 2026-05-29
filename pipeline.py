import argparse
import asyncio
import base64
import email.mime.text
import logging
import os
import uuid

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions

from apache_beam.ml.inference.base import RunInference, PredictionResult
from apache_beam.ml.inference.huggingface_inference import HuggingFacePipelineModelHandler
from apache_beam.ml.inference.vllm_inference import VLLMCompletionsModelHandler
from apache_beam.ml.inference.gemini_inference import GeminiModelHandler, generate_from_string
from apache_beam.ml.inference.agent_development_kit import ADKAgentModelHandler
from apache_beam.transforms.resources import ResourceHint
from google.adk.agents import LlmAgent
from google import genai


# ---------------------------------------------------------------------------
# Backport: PR #38477 — fix race condition in session creation
# https://github.com/apache/beam/pull/38477
#
# In apache-beam 2.73.0 the released ADKAgentModelHandler calls
# session_service.create_session() synchronously inside run_inference()
# before the async gather.  Because create_session() is a coroutine the
# call is never actually awaited, leading to a SessionNotFound error when
# runner.run_async() executes.  The PR moves session creation inside
# _invoke_agent() so it is properly awaited before the agent is started.
#
# This subclass overrides the two affected methods until the fix ships in
# a released version of apache-beam.
# ---------------------------------------------------------------------------
class PatchedADKAgentModelHandler(ADKAgentModelHandler):
    """ADKAgentModelHandler with the race-condition fix from apache/beam#38477.

    Overrides run_inference and _invoke_agent so that session creation is
    awaited correctly before the agent event-loop starts, preventing
    SessionNotFound errors under concurrent batches.
    """

    def __init__(self, agent, project, location):
        super().__init__(agent=agent)
        self.project = project
        self.location = location

    def load_model(self):
        import os
        os.environ["GOOGLE_GENAI_USE_VERTEXAI"] = "TRUE"
        os.environ["GOOGLE_CLOUD_PROJECT"] = self.project
        os.environ["GOOGLE_CLOUD_LOCATION"] = self.location
        return super().load_model()

    def run_inference(self, batch, model, inference_args=None):
        try:
            from google.genai.types import Content as genai_Content
            from google.genai.types import Part as genai_Part
        except ImportError:
            genai_Content = object
            genai_Part = object

        if inference_args is None:
            inference_args = {}

        user_id = inference_args.get("user_id", "beam_user")
        agent_invocations = []
        elements_with_sessions = []

        for element in batch:
            session_id = inference_args.get("session_id", str(uuid.uuid4()))

            # Wrap plain strings in a Content object
            if isinstance(element, str):
                message = genai_Content(role="user", parts=[genai_Part(text=element)])
            else:
                message = element

            # Session creation is now handled inside _invoke_agent (awaited)
            agent_invocations.append(
                self._invoke_agent(model, user_id, session_id, self._app_name, message)
            )
            elements_with_sessions.append(element)

        async def _run_concurrently():
            return await asyncio.gather(*agent_invocations)

        response_texts = asyncio.run(_run_concurrently())

        results = []
        for i, element in enumerate(elements_with_sessions):
            results.append(
                PredictionResult(
                    example=element,
                    inference=response_texts[i],
                    model_id=model.agent.name,
                )
            )
        return results

    async def _invoke_agent(self, runner, user_id, session_id, app_name, message):
        """Drives the ADK event loop with session creation properly awaited.

        Backport of apache/beam#38477: creates the session before starting
        runner.run_async() so the session always exists when the agent
        attempts to read it.
        """
        # Await session creation before starting the agent (the fix)
        # Check for your specific session ID
        try:
            # Attempt to get the specific session
            await runner.session_service.get_session(session_id)
        except Exception as e:
            await runner.session_service.create_session(
                app_name=app_name,
                user_id=user_id,
                session_id=session_id,
            )

        final_response_text = None
        async for event in runner.run_async(
                user_id=user_id,
                session_id=session_id,
                new_message=message,
            ):
            if event.is_final_response():
                if event.content and event.content.parts:
                    final_response_text = "".join([p.text for p in event.content.parts])

        if final_response_text is not None:
            return final_response_text

        raise ValueError(f"Agent {runner.agent.name} did not return a response")

# ---------------------------------------------------------------------------
# Shared DoFns
# ---------------------------------------------------------------------------

class FilterNegativeAndPrompt(beam.DoFn):
    """Filters to NEGATIVE-sentiment messages and yields a remediation prompt.
    Used by the Gemma and Gemini backends.
    Messages can be plain text (no user ID required).
    """

    def process(self, element):
        example = element.example
        inference = element.inference

        if isinstance(inference, list) and len(inference) > 0:
            result = inference[0]
        else:
            result = inference

        label = result.get('label', 'UNKNOWN') if isinstance(result, dict) else 'UNKNOWN'
        logging.warning(f"[SentimentResult] Message: {example!r} | Label: {label}")

        if label == "NEGATIVE":
            prompt = (
                f"Customer message: \"{example}\"\n\n"
                "Recommend an action to remediate this negative customer sentiment. "
                "The actions available are:\n"
                "- Issue a refund. This should only happen if they mention receiving a defective product.\n"
                "- Apologize for bad service. This should happen if the consumer sentiment is bad, but not terrible.\n"
                "- Escalate to management. This should happen if the consumer sentiment is very bad.\n\n"
                "Action:"
            )
            yield prompt


class FilterNegativeAndPromptADK(beam.DoFn):
    """Filters to NEGATIVE-sentiment messages and yields a data-enriched prompt.
    Used by the ADK backend only.

    Messages MUST be formatted as "<user_id>: <message text>", e.g.:
        "3: I received a broken item and I'm furious"

    Messages that cannot be parsed are logged as warnings and dropped.
    """

    def process(self, element):
        example = element.example
        inference = element.inference

        if isinstance(inference, list) and len(inference) > 0:
            result = inference[0]
        else:
            result = inference

        label = result.get('label', 'UNKNOWN') if isinstance(result, dict) else 'UNKNOWN'
        logging.warning(f"[SentimentResult] Message: {example!r} | Label: {label}")

        if label == "NEGATIVE":
            parts = example.split(":", 1)
            if len(parts) != 2 or not parts[0].strip().isdigit():
                logging.warning(
                    "[ADK] Could not parse a user ID from message: %r. "
                    "Messages for the ADK backend must be formatted as "
                    "'<user_id>: <message text>' (e.g. '3: I received a broken item'). "
                    "Filtering out this message.",
                    example,
                )
                return

            user_id = int(parts[0].strip())
            message_text = parts[1].strip()

            prompt = (
                f"User ID: {user_id}\n"
                f"Customer message: \"{message_text}\"\n\n"
                "The customer has expressed negative sentiment. Please take the following steps:\n"
                "1. Call lookup_user(user_id) to retrieve the customer's email address.\n"
                "2. Call lookup_orders(user_id) to retrieve their recent orders and current product inventory.\n"
                "3. Based on the message content and the inventory data, choose the best remediation action:\n"
                "   - 'Ship a replacement' if they mention a defective or broken product AND the "
                "product's remaining_inventory > 5.\n"
                "   - 'Issue a refund' if they mention a defective or broken product but inventory <= 5.\n"
                "   - 'Apologize for bad service' if sentiment is bad but not related to a specific product defect.\n"
                "   - 'Escalate to management' if the sentiment is extremely negative.\n"
                "4. Call send_email(to_address, subject, body) to notify the customer of the chosen action.\n"
                "5. Return a string summarizing everything you did and what responses you got. If anything went wrong, make that clear. Explicitly call out which tools you used."
            )
            yield prompt


class LogLlmResponse(beam.DoFn):
    """Logs inference results from the Gemma and Gemini backends."""

    def process(self, element):
        inference = element.inference
        inference_str = (
            inference[0].outputs[0].text
            if hasattr(inference, "__getitem__") and hasattr(inference[0], "outputs")
            else str(inference)
        )
        logging.warning(f"\n[Remediation Action] for '{element.example}'\n=> {inference_str}")
        yield element


class LogADKResponse(beam.DoFn):
    """Logs the final text response returned by the ADK agent."""

    def process(self, element):
        # ADKAgentModelHandler returns the agent's final text as element.inference
        inference_str = str(element.inference)
        logging.warning(f"\n[ADK Remediation] for '{element.example}'\n=> {inference_str}")
        yield element


# ---------------------------------------------------------------------------
# ADK tool factory
# ---------------------------------------------------------------------------

def make_adk_tools(
    project: str,
    dataset: str = "sentiment_demo",
    user_access_token: str = None,
    user_refresh_token: str = None,
    user_client_id: str = None,
    user_client_secret: str = None,
):
    """Returns a list of ADK tool callables for the remediation agent.

    Builds BigQuery-backed lookup tools and a Gmail-based email sender.
    The GCP project and dataset are captured as closure variables so that
    individual tool functions have clean, agent-visible signatures.

    Args:
        project: GCP project ID used for BigQuery queries.
        dataset: BigQuery dataset name containing the demo tables.
        user_access_token: Optional OAuth access token for the Gmail API.
        user_refresh_token: Optional OAuth refresh token for the Gmail API.
        user_client_id: Optional OAuth client ID for the Gmail API.
        user_client_secret: Optional OAuth client secret for the Gmail API.

    Returns:
        A list of callables: [lookup_user, lookup_orders, send_email]
    """

    def lookup_user(user_id: int) -> dict:
        """Look up user information (email address) from BigQuery by user ID.

        Args:
            user_id: The integer user ID to look up.

        Returns:
            A dict with 'user_id' (int) and 'user_email' (str), or
            a dict with an 'error' key describing what went wrong.
        """
        from google.cloud import bigquery  # imported here to avoid worker serialisation issues

        client = bigquery.Client(project=project)
        query = (
            f"SELECT user_id, user_email "
            f"FROM `{project}.{dataset}.users` "
            f"WHERE user_id = @user_id"
        )
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("user_id", "INT64", user_id)]
        )
        try:
            results = list(client.query(query, job_config=job_config).result())
            if results:
                row = results[0]
                return {"user_id": row.user_id, "user_email": row.user_email}
            return {"error": f"No user found with user_id={user_id}"}
        except Exception as exc:  # pylint: disable=broad-except
            return {"error": str(exc)}

    def lookup_orders(user_id: int) -> dict:
        """Look up a user's orders and current product inventory from BigQuery.

        Joins the purchases table with the products table to return per-order
        inventory and pricing information, which the agent uses to decide
        whether a replacement can be shipped.

        Args:
            user_id: The integer user ID whose orders should be retrieved.

        Returns:
            A dict with an 'orders' list. Each entry contains:
              order_id (int), product_id (int),
              remaining_inventory (int), price (float).
            On error, returns a dict with an 'error' key.
        """
        from google.cloud import bigquery

        client = bigquery.Client(project=project)
        query = (
            f"SELECT p.order_id, p.product_id, pr.remaining_inventory, pr.price "
            f"FROM `{project}.{dataset}.purchases` p "
            f"JOIN `{project}.{dataset}.products` pr ON p.product_id = pr.product_id "
            f"WHERE p.user_id = @user_id"
        )
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("user_id", "INT64", user_id)]
        )
        try:
            results = list(client.query(query, job_config=job_config).result())
            orders = [
                {
                    "order_id": row.order_id,
                    "product_id": row.product_id,
                    "remaining_inventory": row.remaining_inventory,
                    "price": float(row.price),
                }
                for row in results
            ]
            return {"orders": orders}
        except Exception as exc:  # pylint: disable=broad-except
            return {"error": str(exc)}

    def send_email(to_address: str, subject: str, body: str) -> str:
        """Send a plain-text email to the customer via the Gmail API.

        Requires that Application Default Credentials (ADC) include the
        https://www.googleapis.com/auth/gmail.send scope.  When running
        locally, authenticate with:
          gcloud auth application-default login \\
            --scopes=openid,https://www.googleapis.com/auth/cloud-platform,\\
                     https://www.googleapis.com/auth/gmail.send

        Args:
            to_address: Recipient email address.
            subject: Email subject line.
            body: Plain-text email body.

        Returns:
            'Email sent successfully' on success, or an error description.
        """
        import google.auth
        import google.oauth2.credentials
        import googleapiclient.discovery

        try:
            if user_access_token or user_refresh_token:
                creds = google.oauth2.credentials.Credentials(
                    token=user_access_token,
                    refresh_token=user_refresh_token,
                    client_id=user_client_id,
                    client_secret=user_client_secret,
                    token_uri="https://oauth2.googleapis.com/token",
                    quota_project_id=project,
                )
            else:
                creds, _ = google.auth.default(
                    scopes=["https://www.googleapis.com/auth/gmail.send"]
                )
            service = googleapiclient.discovery.build("gmail", "v1", credentials=creds)

            mime_msg = email.mime.text.MIMEText(body)
            mime_msg["to"] = to_address
            mime_msg["subject"] = subject
            raw = base64.urlsafe_b64encode(mime_msg.as_bytes()).decode("utf-8")
            service.users().messages().send(userId="me", body={"raw": raw}).execute()
            return "Email sent successfully"
        except Exception as exc:  # pylint: disable=broad-except
            return f"Failed to send email: {exc}"

    return [lookup_user, lookup_orders, send_email]


# ---------------------------------------------------------------------------
# Pipeline entry-point
# ---------------------------------------------------------------------------

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input_topic',
        dest='input_topic',
        required=True,
        help='Input Pub/Sub topic to read from. Format: projects/<project>/topics/<topic>'
    )
    parser.add_argument(
        '--inference_backend',
        dest='inference_backend',
        default='gemma',
        choices=['gemma', 'gemini', 'adk'],
        help=(
            'Inference backend for remediation: '
            '"gemma" (local vLLM on GPU), '
            '"gemini" (Vertex AI API), or '
            '"adk" (ADK agent with BigQuery tools).'
        )
    )
    parser.add_argument(
        '--user_access_token',
        dest='user_access_token',
        default=None,
        help='OAuth access token used to authenticate Gmail API.'
    )
    parser.add_argument(
        '--user_refresh_token',
        dest='user_refresh_token',
        default=None,
        help='OAuth refresh token used to authenticate Gmail API.'
    )
    parser.add_argument(
        '--user_client_id',
        dest='user_client_id',
        default=None,
        help='OAuth client ID used to authenticate Gmail API.'
    )
    parser.add_argument(
        '--user_client_secret',
        dest='user_client_secret',
        default=None,
        help='OAuth client secret used to authenticate Gmail API.'
    )

    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(StandardOptions).streaming = True
    pipeline_options.view_as(SetupOptions).save_main_session = True

    model_handler = HuggingFacePipelineModelHandler(
        task="sentiment-analysis",
        model="distilbert-base-uncased-finetuned-sst-2-english"
    )

    with beam.Pipeline(options=pipeline_options) as p:
        # Shared: read, decode, and run sentiment analysis on every message.
        sentiment_results = (
            p
            | "ReadFromPubSub" >> beam.io.ReadFromPubSub(topic=known_args.input_topic)
            | "DecodeMessages" >> beam.Map(lambda x: x.decode('utf-8'))
            | "SentimentInference" >> RunInference(model_handler)
        )

        if known_args.inference_backend == 'gemini':
            logging.info("Using Gemini (Vertex AI) as inference backend.")
            gemini_handler = GeminiModelHandler(
                model_name='gemini-2.5-flash',
                request_fn=generate_from_string,
                project=pipeline_options.get_all_options().get('project'),
                location='us-central1',
            )
            _ = (
                sentiment_results
                | "FilterNegative" >> beam.ParDo(FilterNegativeAndPrompt())
                | "GeminiInference" >> RunInference(gemini_handler)
                | "LogResults" >> beam.ParDo(LogLlmResponse())
            )

        elif known_args.inference_backend == 'adk':
            logging.info("Using ADK agent as inference backend.")
            project = pipeline_options.get_all_options().get('project')
            region = pipeline_options.get_all_options().get('region')
            if not project:
                raise ValueError('Project is none')
            if not region:
                raise ValueError('Region is none')

            # Configure the ADK agent to use Vertex AI to avoid "No API key" errors
            os.environ["GOOGLE_GENAI_USE_VERTEXAI"] = "TRUE"
            os.environ["GOOGLE_CLOUD_PROJECT"] = project
            os.environ["GOOGLE_CLOUD_LOCATION"] = region

            adk_tools = make_adk_tools(
                project=project,
                user_access_token=known_args.user_access_token,
                user_refresh_token=known_args.user_refresh_token,
                user_client_id=known_args.user_client_id,
                user_client_secret=known_args.user_client_secret,
            )

            adk_agent = LlmAgent(
                name="remediation_agent",
                model="gemini-2.5-flash",
                instruction=(
                    "You are a customer service remediation assistant with access to "
                    "BigQuery lookup tools and an email sending tool. "
                    "When given a prompt describing a customer situation, follow the "
                    "numbered steps exactly and use your tools to complete the task."
                ),
                tools=adk_tools,
            )
            adk_handler = PatchedADKAgentModelHandler(agent=adk_agent, project=project, location=region)
            _ = (
                sentiment_results
                | "FilterNegativeADK" >> beam.ParDo(FilterNegativeAndPromptADK())
                | "ADKInference" >> RunInference(adk_handler)
                | "LogADKResults" >> beam.ParDo(LogADKResponse())
            )

        else:
            logging.info("Using Gemma (local vLLM) as inference backend.")
            gemma_handler = VLLMCompletionsModelHandler(
                'google/gemma-2b-it',
                vllm_server_kwargs={
                    'dtype': 'half',
                    'max-num-seqs': '32',
                    'gpu-memory-utilization': '0.72',
                    'enforce-eager': 'True',
                    'swap-space': '1',
                }
            )
            _ = (
                sentiment_results
                | "FilterNegative" >> beam.ParDo(FilterNegativeAndPrompt())
                | "ReshuffleForGPU" >> beam.Reshuffle()
                | "GemmaInference" >> RunInference(gemma_handler).with_resource_hints(
                    accelerator="type:nvidia-tesla-t4;count:1;install-nvidia-driver"
                )
                | "LogResults" >> beam.ParDo(LogLlmResponse())
            )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
