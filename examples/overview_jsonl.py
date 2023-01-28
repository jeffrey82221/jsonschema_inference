"""
Get schema of jsonl
& develop analysis platform on it.
"""
import pprint
from jsonschema_inference.inference import JsonlInferenceEngine


class Engine(JsonlInferenceEngine):
    @property
    def jsonl_path(self):
        return 'data/small_test.jsonl'


schema = Engine(inference_worker_cnt=1).get_schema()
pprint.pprint(schema)


class TestEngine(JsonlInferenceEngine):
    @property
    def jsonl_path(self):
        return 'data/small_test.jsonl'


schema = TestEngine(inference_worker_cnt=4).get_schema()
pprint.pprint(schema)
