"""
A basic json schema inference engine
"""
import typing
from ..fitter import fit
from ..objs import JsonSchema
from .reduce import reduce_schema


__all__ = ['InferenceEngine']


class InferenceEngine:
    def __init__(self, batch_size=100):
        self._batch_size = batch_size

    def get_schema_iteratively(self, json_pipe: typing.Iterable[typing.Any]):
        batch_pipe = InferenceEngine._batchwise_generator(json_pipe, batch_size=self._batch_size)
        schema_pipe = map(lambda batch: InferenceEngine.get_schema(batch), batch_pipe)
        return reduce_schema(schema_pipe)
    
    @staticmethod
    def get_schema(json_batch: typing.List[typing.Any]) -> JsonSchema:
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
