from ..config import config
from .objs import Record, Array, Atomic, Union


def fit(data):
    if isinstance(data, dict):
        schema_content = dict()
        for key in data:
            schema_content[key] = fit(
                data[key]
            )
        schema = Record(schema_content)
        if config.unify_records:
            schema = try_unify_dict(schema)
    elif isinstance(data, list):
        schema = Array(Union.set([fit(e) for e in data]))
    elif data is None:
        schema = Atomic(None)
    else:
        schema = Atomic(type(data))
    return schema


def try_unify_dict(dict_schema):
    uni_dict = dict_schema.to_uniform_dict()
    if isinstance(uni_dict._content, Record) or isinstance(
            uni_dict._content, Array):
        return uni_dict
    else:
        return dict_schema
