from jsonschema_inference.schema.objs import Dict, List, Simple, Optional, Union, UniformDict, Unknown
from jsonschema_inference.schema.fitter import fit, try_unify_dict


def test_fit():
    assert fit(1) == Simple(int)
    assert fit(1.2) == Simple(float)
    assert fit([1]) == List(Simple(int))
    assert fit({'a': 1, 'b': 2}) == Dict({'a': Simple(int), 'b': Simple(int)})
    assert fit([1, None]) == List(Optional(Simple(int)))
    assert fit([1, 1.2, None]) == List(
        Union({Simple(int), Simple(float), Simple(None)}))
    assert fit([{'a': 1, 'b': 2}, {'a': 1, 'b': 5}]) == List(
        Dict({'a': Simple(int), 'b': Simple(int)}))
    assert fit([]) == List(Unknown())


def test_unify_dict_callback():
    assert fit({'1': 1, '2': 2}, unify_callback=try_unify_dict) == Dict(
        {'1': Simple(int), '2': Simple(int)})
    assert fit(
        {'1': {'a': 5, 'b': 6}, '2': {'a': 34, 'b': None}},
        unify_callback=try_unify_dict
    ) == UniformDict(Dict({'a': Simple(int), 'b': Optional(Simple(int))}))
    assert fit(
        {'1': [{'a': 5, 'b': 6}], '2': [{'a': 34, 'b': None}]},
        unify_callback=try_unify_dict
    ) == UniformDict(List(Dict({'a': Simple(int), 'b': Optional(Simple(int))})))
