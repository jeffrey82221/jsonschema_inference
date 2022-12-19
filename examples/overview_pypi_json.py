"""
Overview pypi json file
"""
import typing
import pprint
from jsonschema_inference.inference import APIInferenceEngine


class PypiPackageSchemaInferencer(APIInferenceEngine):
    def __init__(self, api_thread_cnt=30, inference_worker_cnt=4, json_per_worker=10, limit=None,
                 cuckoo_dump='pypi_cuckoo.pickle', schema_dump='pypi_schema.pickle'):
        self._limit = limit
        super().__init__(
            api_thread_cnt=api_thread_cnt,
            inference_worker_cnt=inference_worker_cnt,
            json_per_worker=json_per_worker,
            cuckoo_dump=cuckoo_dump,
            schema_dump=schema_dump,
            jsonl_dump='data/archive.jsonl'
        )

    def index_generator(self) -> typing.Iterable[str]:
        with open('data/package_names.txt', 'r') as f:
            for i, pkg in enumerate(map(lambda p: p.strip(), f)):
                if self._limit is not None:
                    if i > self._limit:
                        break
                yield pkg

    def get_url(self, pkg: str) -> str:
        url = f'https://pypi.org/pypi/{pkg}/json'
        return url

    # def is_valid_json(self, json_dict: typing.Dict) -> bool:
    #     return 'info' in json_dict


if __name__ == '__main__':
    p = PypiPackageSchemaInferencer()
    print('number of json:', p.count)
    schema = p.get_schema()
    pprint.pprint(schema)
