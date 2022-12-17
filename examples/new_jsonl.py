import abc
import os
import subprocess
import pprint
from threading import Thread
import signal
import remote
from jsonschema_inference.schema.objs import Union

__all__ = ['JsonlInferenceEngine']

def get_schema_remotely(jsonl_path, verbose=True, position=0):
    import json
    import tqdm
    from jsonschema_inference.schema import InferenceEngine
    total = sum(1 for _ in open(jsonl_path, 'r'))
    with open(jsonl_path, 'r') as f:
        json_pipe = map(json.loads, f)
        if verbose:
            json_pipe = tqdm.tqdm(
                json_pipe, total=total, desc=jsonl_path, position=position)
        schema = InferenceEngine.get_schema(json_pipe)
    return schema

class JsonlInferenceEngine:
    """
    Args:
        - inference_worker_cnt: number of processes inferencing the json schema
        - engine: the python engine for executing the inference process (pypy or python)
        - tmp_dir: the path to store the splitted jsonl files. 
    Methods to be overide:
        - jsonl_path: path to the jsonl file.
    """
    def __init__(self, inference_worker_cnt=8, engine='pypy', tmp_dir='/tmp'):
        self._inference_worker_cnt = inference_worker_cnt
        self._engine = engine
        self._tmp_dir = tmp_dir
        signal.signal(signal.SIGTERM, self._remove_split_files)
        signal.signal(signal.SIGINT, self._remove_split_files)


    @abc.abstractproperty
    def jsonl_path(self):
        raise NotImplementedError

    def get_schema(self, verbose=True):
        self._split_jsonl(self._inference_worker_cnt)
        schemas = []
        def layered_get_schema(i, filename):
            if self._engine == 'pypy':
                decorated_get_schema = remote.pypy(get_schema_remotely)
            else:
                decorated_get_schema = remote.python(get_schema_remotely)
            schema = decorated_get_schema(filename, verbose=verbose, position=i)
            schemas.append(schema)
        try:
            split_file_paths = os.listdir(self._split_path)
            split_file_pipe = map(lambda file: os.path.join(self._split_path, file), split_file_paths)
            # construct the threads
            threads = [Thread(target=layered_get_schema, args=(i, filename))
                    for i, filename in enumerate(split_file_pipe)]
            # start the threads
            for thread in threads:
                thread.daemon=False
                thread.start()
            # wait for the threads to complete
            for i, thread in enumerate(threads):
                thread.join()
                th = threads[i]
                threads[i] = None
                del th
            return Union.set(schemas)
        except BaseException as e:
            for th in threads:
                if th is not None:
                    thread.join()
            raise e
        finally:
            self._remove_split_files()

    def _split_jsonl(self, split_count):
        out = subprocess.check_output(['wc', '-l', self.jsonl_path])
        total = int(out.decode("utf-8").split(self.jsonl_path)[0])
        if not os.path.exists(self._split_path):
            os.mkdir(self._split_path)
        json_cnt_per_file = int(total/split_count)
        subprocess.run(['split', '-l', str(json_cnt_per_file), self.jsonl_path, self._split_path + '/'])
        return json_cnt_per_file

    def _remove_split_files(self, signal=None, frame=None):
        if os.path.exists(self._split_path):
            subprocess.run(['rm', '-r', self._split_path])
            print(self._split_path, 'removed')

    @property
    def _split_path(self):
        return os.path.join(self._tmp_dir, 'split')


class Engine(JsonlInferenceEngine):
    @property
    def jsonl_path(self):
        return 'data/kaggle_data/test.jsonl'

if __name__ == '__main__':
    engine = Engine()
    schema = engine.get_schema(verbose=True)
    pprint.pprint(schema)