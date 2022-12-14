"""
Get schema of jsonl
& develop analysis platform on it.
"""
import pprint
from common.inference import JsonlInferenceEngine


class Engine(JsonlInferenceEngine):
    @property
    def jsonl_path(self):
        return 'data/small_test.jsonl'


schema = Engine(inference_worker_cnt=1, json_per_worker=1).get_schema()
pprint.pprint(schema)


class TestEngine(JsonlInferenceEngine):
    @property
    def jsonl_path(self):
        return 'data/kaggle_data/test.jsonl'


schema = TestEngine(inference_worker_cnt=8, json_per_worker=10000).get_schema()
pprint.pprint(schema)