"""
A basic json schema inference engine
"""
from ..fitter import fit, try_unify_dict
from ..objs import Union, JsonSchema


__all__ = ['InferenceEngine']


class InferenceEngine:
    @staticmethod
    def get_schema(json_batch) -> JsonSchema:
        return Union.set(
            map(lambda js: fit(js, unify_callback=try_unify_dict), json_batch))

    @staticmethod
    def _batchwise_generator(gen, batch_size=100):
        batch = []
        for i, element in enumerate(gen):
            batch.append(element)
            if i % batch_size == (batch_size - 1):
                yield batch
                del batch
                batch = []
        if batch:
            yield batch
