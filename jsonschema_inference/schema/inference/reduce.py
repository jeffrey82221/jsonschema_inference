import typing
from functools import reduce
from ..objs.basic import JsonSchema
from ..objs.basic import Unknown


def reduce_schema(json_schemas: typing.Iterable[JsonSchema]) -> JsonSchema:
    if json_schemas:
        result = reduce(lambda a, b: a | b, json_schemas)
    else:
        result = Unknown()
    return result
