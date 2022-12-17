"""
A Json schema builder from multiple strutured Json files

Refactor: 
- [ ] alter the multi-processing method by seperate jsonl into multiple files and execute parallelly
 using pypy
"""
import abc
import typing
import tqdm
from multiprocessing.pool import ThreadPool
import json
from ..schema.objs import Union
from ..schema import InferenceEngine

__all__ = ['JsonlInferenceEngine']


class JsonlInferenceEngine:
    """
    Args:
        - inference_worker_cnt: number of processes inferencing the json schema
        - json_per_worker: number of json files an inference worker takes as input
    Methods to be overide:
        - jsonl_path: path to the jsonl file.
    """
    def __init__(self, inference_worker_cnt=8, json_per_worker=10000):
        self._inference_worker_cnt = inference_worker_cnt
        if self._inference_worker_cnt > 1:
            from ray.util.multiprocessing import Pool
            self.Pool = Pool
        else:
            self.Pool = ThreadPool
        self._json_per_worker = json_per_worker

    @abc.abstractproperty
    def jsonl_path(self):
        raise NotImplementedError

    def get_schema(self, verbose=True):
        if verbose:
            total = sum(1 for _ in open(self.jsonl_path, 'r'))
        with open(self.jsonl_path, 'r') as f:
            with self.Pool(processes=self._inference_worker_cnt) as pr_exc:
                json_pipe = map(json.loads, f)
                if verbose:
                    json_pipe = tqdm.tqdm(
                        json_pipe, total=total, desc=self.jsonl_path)
                json_batch_pipe = InferenceEngine._batchwise_generator(
                    json_pipe, batch_size=self._json_per_worker)
                # Inferencing Json schemas from Json Batches
                json_schema_pipe = pr_exc.imap_unordered(
                    JsonlInferenceEngine._pr_run, json_batch_pipe)
                if verbose:
                    json_schema_pipe = tqdm.tqdm(json_schema_pipe, total=int(total / self._json_per_worker),
                                                 desc='batch-wise schema')
                schema = Union.set(json_schema_pipe)
        return schema

    @staticmethod
    def _pr_run(
            json_batch: typing.List[typing.Dict]):
        json_schema = InferenceEngine.get_schema(json_batch)
        return json_schema
