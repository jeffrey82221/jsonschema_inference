import abc
import os
import subprocess
import pprint
from threading import Thread
import signal
from . import remote
from ..schema.objs import Union

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
        - tmp_dir: the path to store the splitted jsonl files.
    Methods to be overide:
        - jsonl_path: path to the jsonl file.
    """

    def __init__(self, inference_worker_cnt=8, tmp_dir='/tmp'):
        self._inference_worker_cnt = inference_worker_cnt
        self._tmp_dir = tmp_dir
        # NOTE: Whether the engine is pypy or python depends on how the code is
        # executed
        self._engine = 'pypy'  # or python
        if inference_worker_cnt > 1:
            signal.signal(signal.SIGTERM, self._graceful_exit)
            signal.signal(signal.SIGINT, self._graceful_exit)

    @abc.abstractproperty
    def jsonl_path(self):
        raise NotImplementedError

    def get_schema(self, verbose=True):
        if self._inference_worker_cnt == 1:
            result = get_schema_remotely(self.jsonl_path, verbose=verbose)
        else:
            result = self.get_schema_parallel(verbose=verbose)
        return result

    def get_schema_parallel(self, verbose=True):
        self._split_jsonl(self._inference_worker_cnt)
        schemas = []
        self._remote_gateways = []

        def layered_get_schema(i, filename):
            if self._engine == 'pypy':
                gw, decorated_get_schema = remote.pypy(get_schema_remotely)
            else:
                gw, decorated_get_schema = remote.python(get_schema_remotely)
            self._remote_gateways.append(gw)
            schema = decorated_get_schema(
                filename, verbose=verbose, position=i)
            schemas.append(schema)
        try:
            split_file_paths = os.listdir(self._split_path)
            split_file_pipe = map(
                lambda file: os.path.join(
                    self._split_path,
                    file),
                split_file_paths)
            # construct the threads
            self.threads = [Thread(target=layered_get_schema, args=(i, filename))
                            for i, filename in enumerate(split_file_pipe)]
            # start the threads
            for thread in self.threads:
                thread.daemon = False
                thread.start()
            # wait for the threads to complete
            for i, thread in enumerate(self.threads):
                thread.join()
                th = self.threads[i]
                self.threads[i] = None
                del th
            result = Union.set(schemas)
            return result
        except BaseException as e:
            raise e
        finally:
            self._exit()

    def _exit(self):
        self.__stop_gateways()
        self.__remove_split_files()
        print('exit')

    def _graceful_exit(self, signal=None, frame=None):
        self._exit()
        print(f'graceful exited')
        os._exit(0)

    def __stop_gateways(self):
        for gw in self._remote_gateways:
            try:
                gw.exit()
            except BaseException:
                pass
        print(f'remote gateways stopped')

    def __remove_split_files(self):
        if os.path.exists(self._split_path):
            subprocess.run(['rm', '-r', self._split_path])
            print(f'split files removed')

    def _split_jsonl(self, split_count):
        out = subprocess.check_output(['wc', '-l', self.jsonl_path])
        total = int(out.decode("utf-8").split(self.jsonl_path)[0])
        if not os.path.exists(self._split_path):
            os.mkdir(self._split_path)
        json_cnt_per_file = int(total / split_count)
        subprocess.run(['split', '-l', str(json_cnt_per_file),
                       self.jsonl_path, self._split_path + '/'])
        return json_cnt_per_file

    @property
    def _split_path(self):
        return os.path.join(self._tmp_dir, 'split')
