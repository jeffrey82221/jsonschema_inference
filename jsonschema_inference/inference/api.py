"""
A Json schema builder from multiple strutured Json files

NOTE:

What is a structured JSON file?

It is a dictionary with key and elements

Elements can be `List`, `Dictionary`, or `Simple Variables`, which is a value or string (Optional[int], Optional[float], ...).

If the element is a `List`, it must have elements of same `Simple Variables` or `Dictionary` with same keys.

If the element is a `Dictionary`, it should have fixed keys and fixed type of element.

If the element is a `Simple Variables`, it should have fixed type.

TODO:
- [X] Build another create_schema with keys of dictionary shown
- [X] Generate consistet schema to allow multiple json's schema to be merged into a more consistent schema
- [X] Convert Schema to a Python DataClass
    - [X] A class extract json properties one-by-one
    - [X] A class generate overall json content
- [X] An adaptor that takes json as input and initialize the python DataClass

- [-] Change redis-server cache to another one


TODO:
- [X] Use Python Cuckoo Filter to avoid repeat download of json
- [ ] enable no dump mode

REF:
https://github.com/huydhn/cuckoo-filter
https://dl.acm.org/doi/pdf/10.1145/2674005.2674994

"""
import abc
import os
import pickle
import math
import logging
import signal
import sys
import typing
import requests
import tqdm
from multiprocessing.pool import ThreadPool
import json as json_package
from ..schema.objs import JsonSchema
from ..schema import InferenceEngine

__all__ = ['APIInferenceEngine']


class APIInferenceEngine:
    """
    Args:
        - api_thread_cnt: number of threads downloading json from url
        - inference_worker_cnt: number of processes inferencing the json schema
        - json_per_worker: number of json files an inference worker takes as input
        - cuckoo_dump: path to a dump file to store the record of processed index
        - schema_dump: path to a dump to store the inferenced json schema.
    Methods to be overide:
        - index_generator: a generator yeilding index (or url) strings referencing to a json file
        - index_to_url: a function takes the index from index_generator as input and convert it to an url
            (If index_generator yeilds url, no need to overide this function.)
        - is_json_valid: a function takes determine whether a json is valid or not
            (The invlid json would be ignored.)
    """

    def __init__(self, api_thread_cnt=1000, inference_worker_cnt=4, json_per_worker=1000,
                 cuckoo_dump='cuckoo.pickle', schema_dump='schema.pickle', jsonl_dump=None):
        self._api_thread_cnt = api_thread_cnt
        self._inference_worker_cnt = inference_worker_cnt
        if self._inference_worker_cnt > 1:
            from ray.util.multiprocessing import Pool
            self.Pool = Pool
        else:
            self.Pool = ThreadPool
        self._json_per_worker = json_per_worker
        self._index_filter = IndexCuckooFilter(
            self.index_generator, dump_file_path=cuckoo_dump)
        self._schema_holder = SchemaReducer(
            self._index_filter, dump_file_path=schema_dump)
        self._register_graceful_exist(
            [self._index_filter, self._schema_holder])
        self._jsonl_dump = jsonl_dump
        if self._jsonl_dump is not None:
            self._jsonl_index_filter = IndexCuckooFilter(
                self.index_generator, dump_file_path='jsonl_cuckoo.pickle'
            )
            self._jsonl_saver = JsonlSaver(
                self._jsonl_index_filter,
                archieve_file_path=jsonl_dump)
            self._register_graceful_exist([self._jsonl_index_filter])

    def _register_graceful_exist(self, objs):
        def do_exit(*args):
            for p in objs:
                p.exit_gracefully(*args)
            sys.exit(1)
        signal.signal(signal.SIGINT, do_exit)
        signal.signal(signal.SIGTERM, do_exit)

    @abc.abstractmethod
    def index_generator(self) -> typing.Iterable[str]:
        raise NotImplementedError

    @abc.abstractmethod
    def get_url(self, index: str) -> str:
        raise NotImplementedError

    @abc.abstractmethod
    def is_valid_json(self, json_dict: typing.Dict) -> bool:
        return True

    @property
    def count(self):
        return sum(1 for _ in self.index_generator())

    def get_schema(self, verbose=True):
        """
        A pipeline for inferencing json schema from a
        json files generated from url indices.
        """
        # Get indices (ignroe already processed ones)
        indexs = list(self._index_filter.filter(self.index_generator()))
        index_name_pipe = indexs
        try:
            with ThreadPool(processes=self._api_thread_cnt) as th_exc:
                with self.Pool(processes=self._inference_worker_cnt) as pr_exc:
                    if verbose:
                        index_name_pipe = tqdm.tqdm(
                            index_name_pipe,
                            desc='in-dkg-flow'
                        )
                    # Mapping index to URL
                    url_index_name_pipe = map(
                        lambda index: (
                            self.get_url(index),
                            index),
                        index_name_pipe)

                    # Download Json from URL
                    json_index_name_pipe = th_exc.imap_unordered(
                        APIInferenceEngine._th_run, url_index_name_pipe)

                    if verbose:
                        json_index_name_pipe = tqdm.tqdm(
                            json_index_name_pipe, total=len(indexs),
                            desc='json-flow')

                    # Remove errorneous Json
                    json_index_name_pipe = self.filter_errorneous_json(
                        json_index_name_pipe)

                    # Saving json into jsonl file
                    if self._jsonl_dump is not None:
                        json_index_name_pipe = self._jsonl_saver.save(
                            json_index_name_pipe)

                    # Convert to batch-wise pipe
                    json_index_name_batch_pipe = InferenceEngine._batchwise_generator(
                        json_index_name_pipe, batch_size=self._json_per_worker)

                    # Inferencing Json schemas from Json Batches
                    json_schema_indexs_pipe = pr_exc.imap_unordered(
                        APIInferenceEngine._pr_run, json_index_name_batch_pipe)

                    if verbose:
                        json_schema_indexs_pipe = tqdm.tqdm(
                            json_schema_indexs_pipe, total=round(
                                len(indexs) / self._json_per_worker),
                            desc='schema-batch-flow')

                    # Reducing Json Schemas into One Union Json Schema
                    self._schema_holder.reduce(json_schema_indexs_pipe)
            return self._schema_holder.union_schema
        except BaseException as e:
            raise e
        finally:
            # Saving the final schema and process record as Pickles
            self._schema_holder.save()
            self._index_filter.save()
            if self._jsonl_dump is not None:
                self._jsonl_index_filter.save()

    @staticmethod
    def _th_run(instance):
        url, index = instance
        json_result = APIInferenceEngine._get_json(url)
        return json_result, index

    @staticmethod
    def _get_json(url):
        result = requests.get(url).json()
        return result

    def filter_errorneous_json(
            self, json_index_name_pipe: typing.Iterable[typing.Tuple[typing.Dict, str]]):
        def remove_index_of_errorneous_json(instance):
            json_result, index = instance
            if not self.is_valid_json(json_result):
                self._index_filter.remove(index)
            return json_result, index
        json_index_name_pipe = map(
            remove_index_of_errorneous_json,
            json_index_name_pipe)
        json_index_name_pipe = filter(
            lambda x: self.is_valid_json(x[0]), json_index_name_pipe)
        return json_index_name_pipe

    @staticmethod
    def _pr_run(
            json_index_name_batch: typing.List[typing.Tuple[typing.Dict, str]]):
        index_name_batch = list(map(lambda x: x[1], json_index_name_batch))
        json_schema = InferenceEngine.get_schema(
            map(lambda x: x[0], json_index_name_batch))
        return json_schema, index_name_batch


class IndexCuckooFilter:
    """
    Filter out index whose json schema
    has already been captured in the downstream pipeline.

    Args:
        - index_gen_builder: a function that produce a index generator which help IndexCuckooFilter to
            construct the index set.
        - dump_file_path: the path to store the index set status information.
        - error_rate: the error rate of identifying an non-existing item in the set.
    """

    def __init__(self, index_gen_builder=typing.Callable[[
    ], typing.Iterable], dump_file_path='cuckoo.pickle', error_rate: float = 0.01):
        self._dump_file_path = dump_file_path
        if os.path.exists(self._dump_file_path):
            self._cuckoo = self.load()
        else:
            # build the cuckoo filter from scratch
            indexs = list(index_gen_builder())
            assert error_rate <= 1. and error_rate > 0.
            bucket_size = round(- math.log10(error_rate)) + 1
            # fingerprint_size = int(math.ceil(math.log(1.0 / error_rate, 2) + math.log(2 * bucket_size, 2)))
            if bucket_size == 1:
                alpha = 0.5
            elif bucket_size >= 2 and bucket_size < 4:
                alpha = 0.84
            elif bucket_size >= 4 and bucket_size < 8:
                alpha = 0.95
            elif bucket_size >= 8:
                alpha = 0.98
            capacity = round(len(indexs) / alpha)
            logging.info('capacity of cuckoo filter:', capacity)
            logging.info(
                'false positive error rate of cuckoo filter:',
                error_rate)
            logging.info('bucket size of cuckoo filter:', bucket_size)
            from cuckoo.filter import CuckooFilter
            self._cuckoo = CuckooFilter(
                capacity=capacity,
                error_rate=error_rate,
                bucket_size=bucket_size)
            for index in indexs:
                self._cuckoo.insert(index)

    def remove(self, index: str):
        self._cuckoo.delete(index)

    def filter(self, index_gen: typing.Iterable):
        for index in index_gen:
            if self._cuckoo.contains(index):
                yield index

    def load(self):
        with open(self._dump_file_path, 'rb') as handle:
            result = pickle.load(handle)
        print(f'{self._dump_file_path} Loaded')
        return result

    def exit_gracefully(self, *args):
        self.save()
        print('[PackageCuckooFilter] exit gracefully')

    def save(self):
        with open(self._dump_file_path, 'wb') as handle:
            pickle.dump(self._cuckoo, handle, protocol=pickle.HIGHEST_PROTOCOL)
        print(f'{self._dump_file_path} Saved')


class JsonlSaver:
    """
    This class enable saving the json(s) into a archive jsonl file.
    """

    def __init__(self, cuckoo_filter: IndexCuckooFilter,
                 archieve_file_path='archieve.jsonl'):
        self._cuckoo_filter = cuckoo_filter
        self._archieve_file_path = archieve_file_path

    def save(
            self, json_index_producer: typing.Iterable[typing.Tuple[dict, str]]):
        """
        Check and save the passing index-json tuple
        """
        with open(self._archieve_file_path, 'w') as f:
            for json, index in json_index_producer:
                if self._cuckoo_filter._cuckoo.contains(index):
                    f.write(json_package.dumps(json))
                    f.write('\n')
                    self._cuckoo_filter.remove(index)
                else:
                    pass
                yield json, index


class SchemaReducer:
    """
    This class reduces the json schemas produced by
    the inference_workers.

    It stored the union schema of the inferenced json schemas
    and captured the record the corresponding indices
    in the IndexCuckooFilter.
    """

    def __init__(self, cuckoo_filter: IndexCuckooFilter,
                 dump_file_path='schema.pickle'):
        self._cuckoo_filter = cuckoo_filter
        self._dump_file_path = dump_file_path
        if os.path.exists(self._dump_file_path):
            self._current_schema = self.load()
        else:
            self._current_schema = None

    def reduce(
            self, schema_indices_producer: typing.Iterable[typing.Tuple[JsonSchema, typing.List[str]]]):
        for schema, indices in schema_indices_producer:
            for index in indices:
                self._cuckoo_filter.remove(index)
            if self._current_schema is not None:
                self._current_schema |= schema
            else:
                self._current_schema = schema

    def load(self):
        with open(self._dump_file_path, 'rb') as handle:
            result = pickle.load(handle)
        print(f'{self._dump_file_path} Loaded')
        return result

    def save(self):
        with open(self._dump_file_path, 'wb') as handle:
            pickle.dump(
                self._current_schema,
                handle,
                protocol=pickle.HIGHEST_PROTOCOL)
        print(f'{self._dump_file_path} Saved')

    def exit_gracefully(self, *args):
        self.save()
        print('[SchemaReducer] exit gracefully')

    @property
    def union_schema(self):
        return self._current_schema
