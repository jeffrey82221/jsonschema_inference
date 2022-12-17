import pytest
import copy
from collections import Counter
from jsonschema_inference.schema.objs import Simple, List, Dict, Union, Optional, UniformDict, Unknown, DynamicDict


@pytest.fixture()
def simple_int():
    return Simple(int)


@pytest.fixture()
def simple_float():
    return Simple(float)


@pytest.fixture()
def simple_none():
    return Simple(None)


@pytest.fixture()
def int_list():
    return List(Simple(int))


@pytest.fixture()
def float_list():
    return List(Simple(float))


@pytest.fixture()
def int_float_dict():
    return Dict({'a': Simple(int), 'b': Simple(float)})


@pytest.fixture()
def complex_dict():
    return Dict({'a': Simple(int), 'b': List(Simple(float)), 'c': Dict(
        {'a': Simple(str), 'b': Optional(Simple(int))})})


@pytest.fixture()
def int_float_union():
    return Union({Simple(int), Simple(float)})


@pytest.fixture()
def optional_int():
    return Optional(Simple(int))


@pytest.fixture()
def optional_int_list():
    return Optional(List(Simple(int)))


def test_repr():
    for schema_str in [
        'Simple(int)',
        'Simple(float)',
        'List(Simple(int))',
        "Dict({'a': Simple(int), 'b': Simple(float)})",
        "Dict({'a': Simple(int), 'b': List(Simple(float))})",
        'Optional(Simple(int))',
        'Union({Simple(int), Simple(float)})',
        "DynamicDict({'a': Simple(int), 'b': Simple(float)}, Counter({'a': 4, 'b': 2}))",
        "Unknown()"
    ]:
        assert str(eval(schema_str)) == schema_str


def test_equal(
    simple_int, simple_float, simple_none,
    int_list, float_list, int_float_dict, complex_dict
):
    assert simple_int == copy.deepcopy(simple_int)
    assert simple_float == copy.deepcopy(simple_float)
    assert simple_none == copy.deepcopy(simple_none)
    assert int_list == copy.deepcopy(int_list)
    assert float_list == copy.deepcopy(float_list)
    assert int_float_dict == copy.deepcopy(int_float_dict)
    assert complex_dict == copy.deepcopy(complex_dict)
    assert simple_int != simple_float
    assert int_list != float_list
    assert simple_none != simple_int
    assert simple_int != int_list
    assert complex_dict != int_float_dict
    assert Unknown() == Unknown()
    assert Unknown() != int_list
    assert int_list != Unknown()


def test_union(
    simple_int, simple_float, simple_none,
    int_list, float_list, int_float_dict, complex_dict, int_float_union
):
    assert (Unknown() | simple_int) == simple_int
    assert (simple_int | Unknown()) == simple_int
    assert (Unknown() | Unknown()) == Unknown()
    assert (List(Unknown()) | List(simple_int)) == List(simple_int)
    assert (simple_int | simple_int) == simple_int
    assert (simple_float | simple_float) == simple_float
    assert (float_list | float_list) == float_list
    assert (simple_int | simple_float) == int_float_union
    assert (simple_float | simple_int) == int_float_union
    assert (simple_float | simple_int | simple_int) == int_float_union
    assert (simple_float | int_list) == Union({simple_float, int_list})
    assert (simple_float | int_float_dict) == Union(
        {simple_float, int_float_dict})
    assert (simple_none | simple_int) == Optional(simple_int)
    assert (simple_none | complex_dict) == Optional(complex_dict)
    assert (simple_none | simple_int | simple_int) == Optional(simple_int)
    assert (simple_int | simple_none | simple_int) == Optional(simple_int)
    assert (Optional(simple_int) | Simple(None)) == Optional(simple_int)
    assert (Optional(simple_float) | Optional(
        simple_float)) == Optional(simple_float)
    assert (Optional(simple_float) | simple_int) == Optional(
        Union({simple_int, simple_float}))
    assert (Optional(simple_float) | Optional(simple_int)
            ) == Optional(Union({simple_int, simple_float}))
    assert (simple_int | Optional(simple_float)) == Optional(
        Union({simple_int, simple_float}))
    assert (simple_int | Union({simple_float, simple_int})) == Union(
        {simple_int, simple_float})
    assert float_list | int_list == List(Union({simple_float, simple_int}))
    assert float_list | List(
        Optional(
            Simple(float))) == List(
        Optional(simple_float))
    assert Dict({'a': Simple(int), 'b': Simple(float)}) | Dict({'a': Simple(
        int), 'b': Simple(float)}) == Dict({'a': Simple(int), 'b': Simple(float)})
    assert Dict({'a': Simple(int),
                 'b': Simple(float)}) | Dict({'a': Simple(None),
                                              'b': Simple(float)}) == Dict({'a': Optional(Simple(int)),
                                                                            'b': Simple(float)})
    assert Dict({'a': Simple(int), 'b': Simple(float)}) | Dict(
        {'a': Simple(int)}) == DynamicDict({'a': Simple(int), 'b': Simple(float)}, Counter({'a': 2, 'b': 1}))


def test_set(simple_int, simple_float, simple_none,
             int_list, float_list, int_float_dict, complex_dict, int_float_union
             ):
    assert Union.set([]) == Unknown()
    assert Union.set([simple_int, simple_float]) == int_float_union
    assert Union.set([simple_int, simple_none]) == Optional(simple_int)
    assert Union.set([int_list, float_list, int_float_dict]) == Union(
        {List(Union({simple_int, simple_float})), int_float_dict})
    assert Union.set([complex_dict, complex_dict,
                     complex_dict]) == complex_dict
    assert {complex_dict, complex_dict} == {complex_dict}
    assert len({Optional(simple_int), List(simple_int)}) == 2
    assert len({List(simple_int), List(simple_int)}) == 1
    assert len({Optional(simple_float), List(
        simple_float), UniformDict(simple_float)}) == 3
    assert Union.set([Dict({'a': Simple(int)}), Dict({'a': Simple(int), 'b': Simple(
        float)})]) == DynamicDict({'a': Simple(int), 'b': Simple(float)}, Counter({'a': 2, 'b': 1}))
    assert Union.set([Simple(None), Dict({'a': Simple(int)})]) == Optional(
        Dict({'a': Simple(int)}))
    assert Union.set([Simple(None),
                      Dict({'a': Simple(int)}),
                      Dict({'a': Simple(int),
                            'b': Simple(float)})]) == Optional(DynamicDict({'a': Simple(int),
                                                                            'b': Simple(float)}, Counter({'a': 2, 'b': 1})))


def test_to_uniform_dict(int_float_dict, simple_int, simple_float):
    assert int_float_dict.to_uniform_dict() == UniformDict(
        Union({simple_int, simple_float}))
