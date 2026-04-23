import argparse
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions

from apache_beam.ml.inference.base import RunInference
from apache_beam.ml.inference.huggingface_inference import HuggingFacePipelineModelHandler
from apache_beam.ml.inference.vllm_inference import VLLMCompletionsModelHandler
from apache_beam.ml.inference.gemini_inference import GeminiModelHandler, generate_from_string
from apache_beam.transforms.resources import ResourceHint

class FilterNegativeAndPrompt(beam.DoFn):
    def process(self, element):
        # RunInference yields PredictionResult objects.
        example = element.example
        inference = element.inference
        
        # For HuggingFace pipeline, inference results may be a dict or a list of dicts.
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

class LogGemmaResponse(beam.DoFn):
    def process(self, element):
        inference = element.inference
        inference_str = inference[0].outputs[0].text if hasattr(inference, "__getitem__") and hasattr(inference[0], "outputs") else str(inference)
        logging.warning(f"\n[Remediation Action] for '{element.example}'\n=> {inference_str}")
        yield element

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
        choices=['gemma', 'gemini'],
        help='Inference backend to use for remediation: "gemma" (local vLLM on GPU) or "gemini" (Vertex AI API).'
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
        messages = (
            p
            | "ReadFromPubSub" >> beam.io.ReadFromPubSub(topic=known_args.input_topic)
            | "DecodeMessages" >> beam.Map(lambda x: x.decode('utf-8'))
            | "SentimentInference" >> RunInference(model_handler)
            | "FilterNegative" >> beam.ParDo(FilterNegativeAndPrompt())
        )

        if known_args.inference_backend == 'gemini':
            logging.info("Using Gemini (Vertex AI) as inference backend.")
            gemini_handler = GeminiModelHandler(
                model_name='gemini-2.0-flash-001',
                request_fn=generate_from_string,
                project=pipeline_options.get_all_options().get('project'),
                location='us-central1',
            )
            _ = (
                messages
                | "GeminiInference" >> RunInference(gemini_handler)
                | "LogResults" >> beam.ParDo(LogGemmaResponse())
            )
        else:
            logging.info("Using Gemma (local vLLM) as inference backend.")
            gemma_handler = VLLMCompletionsModelHandler(
                'google/gemma-2b-it',
                vllm_server_kwargs={'dtype': 'half', 'max-num-seqs': '32', 'gpu-memory-utilization': '0.72', 'enforce-eager': 'True', 'swap-space': '1'}
            )
            _ = (
                messages
                | "ReshuffleForGPU" >> beam.Reshuffle()
                | "GemmaInference" >> RunInference(gemma_handler).with_resource_hints(
                    accelerator="type:nvidia-tesla-t4;count:1;install-nvidia-driver"
                )
                | "LogResults" >> beam.ParDo(LogGemmaResponse())
            )
        # alternate model handler: ADKAgentModelHandler(agent=my_adk_agent)

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
