from .objs import Dict, List, Simple, Union


def fit(data, unify_callback=None):
    if isinstance(data, dict):
        schema_content = dict()
        for key in data:
            schema_content[key] = fit(
                data[key],
                unify_callback=unify_callback
            )
        schema = Dict(schema_content)
        if unify_callback is not None:
            schema = unify_callback(schema)
    elif isinstance(data, list):
        schema = List(Union.set([fit(e) for e in data]))
    elif data is None:
        schema = Simple(None)
    else:
        schema = Simple(type(data))
    return schema


def try_unify_dict(dict_schema):
    uni_dict = dict_schema.to_uniform_dict()
    if isinstance(uni_dict._content, Dict) or isinstance(
            uni_dict._content, List):
        return uni_dict
    else:
        return dict_schema
