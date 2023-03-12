"""
A basic json schema inference engine
"""
from ..fitter import fit
from ..objs import JsonSchema
from .reduce import reduce_schema


__all__ = ['InferenceEngine']


class InferenceEngine:
    @staticmethod
    def get_schema(json_batch) -> JsonSchema:
        return reduce_schema(
            map(fit, json_batch))

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
