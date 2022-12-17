import os
import subprocess
import pprint
import json
import tqdm
from remote import pypy, python

JSONL_PATH = 'data/kaggle_data/test.jsonl'

def split_file(count):
    out = subprocess.check_output(['wc', '-l', JSONL_PATH])
    cnt = int(out.decode("utf-8").split(JSONL_PATH)[0])
    if not os.path.exists('/tmp/split'):
        os.mkdir('/tmp/split')
    json_cnt_per_file = int(cnt/count)
    subprocess.run(['split', '-l', str(json_cnt_per_file), JSONL_PATH, '/tmp/split/'])
    return json_cnt_per_file

def run_split_files():
    subprocess.run(['rm', '-r', '/tmp/split'])

@pypy
def get_schema(jsonl_path, verbose=True):
    from jsonschema_inference.schema import InferenceEngine
    total = sum(1 for _ in open(jsonl_path, 'r'))
    with open(jsonl_path, 'r') as f:
        json_pipe = map(json.loads, f)
        if verbose:
            json_pipe = tqdm.tqdm(
                json_pipe, total=total)
        schema = InferenceEngine.get_schema(json_pipe)
    return schema

if __name__ == '__main__':
    _ = split_file(10)
    schema = get_schema('/tmp/split/aa', verbose=True)
    pprint.pprint(schema)
    run_split_files()