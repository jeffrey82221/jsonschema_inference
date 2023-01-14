from jsonschema_inference.schema.objs import Dict, Array, Atomic, Optional, Union, UniformDict, Unknown
from jsonschema_inference.schema.fitter import fit, try_unify_dict


def test_fit():
    assert fit(1) == Atomic(int)
    assert fit(1.2) == Atomic(float)
    assert fit([1]) == Array(Atomic(int))
    assert fit({'a': 1, 'b': 2}) == Dict({'a': Atomic(int), 'b': Atomic(int)})
    assert fit([1, None]) == Array(Optional(Atomic(int)))
    assert fit([1, 1.2, None]) == Array(
        Union({Atomic(int), Atomic(float), Atomic(None)}))
    assert fit([{'a': 1, 'b': 2}, {'a': 1, 'b': 5}]) == Array(
        Dict({'a': Atomic(int), 'b': Atomic(int)}))
    assert fit([]) == Array(Unknown())


def test_unify_dict_callback():
    assert fit({'1': 1, '2': 2}, unify_callback=try_unify_dict) == Dict(
        {'1': Atomic(int), '2': Atomic(int)})
    assert fit(
        {'1': {'a': 5, 'b': 6}, '2': {'a': 34, 'b': None}},
        unify_callback=try_unify_dict
    ) == UniformDict(Dict({'a': Atomic(int), 'b': Optional(Atomic(int))}))
    assert fit(
        {'1': [{'a': 5, 'b': 6}], '2': [{'a': 34, 'b': None}]},
        unify_callback=try_unify_dict
    ) == UniformDict(Array(Dict({'a': Atomic(int), 'b': Optional(Atomic(int))})))
