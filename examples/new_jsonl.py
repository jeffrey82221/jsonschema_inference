import os
import subprocess
import pprint
import json
import tqdm
from jsonschema_inference.schema import InferenceEngine

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

def get_schema(jsonl_path, total, verbose=True):
    with open(jsonl_path, 'r') as f:
        
        json_pipe = map(json.loads, f)
        if verbose:
            json_pipe = tqdm.tqdm(
                json_pipe, total=total)
        schema = InferenceEngine.get_schema(json_pipe)
    return schema

if __name__ == '__main__':
    json_cnt_per_file = split_file(10)
    schema = get_schema('/tmp/split/aa', total=json_cnt_per_file, verbose=True)
    pprint.pprint(schema)
    run_split_files()