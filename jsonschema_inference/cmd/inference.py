import argparse
import autopep8
from jsonschema_inference.inference import JsonlInferenceEngine


def run() -> None:
    parser = argparse.ArgumentParser(
        description='Inferencing Json Schema')

    parser.add_argument('--jsonl',
                        type=str, required=True,
                        help="Inference Json Schema from .jsonl file")

    parser.add_argument('--nworkers',
                        type=int, required=False, default=1,
                        help="Inference Worker Count")

    parser.add_argument('--verbose',
                        type=bool, required=False, default=False,
                        help="Showing the Result by Pretty Print")

    parser.add_argument('--out',
                        type=str, required=False, default='',
                        help="Saving the json schema into a output file")

    args = parser.parse_args()
    print(f"Your json file is at: {args.jsonl}")
    print('verbose:', args.verbose)

    class Engine(JsonlInferenceEngine):
        @property
        def jsonl_path(self):
            return args.jsonl

    schema = Engine(
        inference_worker_cnt=args.nworkers).get_schema(
        verbose=args.verbose)
    schema_str = autopep8.fix_code(str(schema))
    if args.verbose:
        print(schema_str)
    if args.out != '':
        with open(args.out, 'w') as f:
            f.write(schema_str)
