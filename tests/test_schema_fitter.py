from jsonschema_inference.schema.objs import Record, Array, Atomic, Optional, Union, UniformRecord, Unknown
from jsonschema_inference import fit
from jsonschema_inference import init

def test_fit():
    assert fit(1) == Atomic(int)
    assert fit(1.2) == Atomic(float)
    assert fit([1]) == Array(Atomic(int))
    assert fit({'a': 1, 'b': 2}) == Record({'a': Atomic(int), 'b': Atomic(int)})
    assert fit([1, None]) == Array(Optional(Atomic(int)))
    assert fit([1, 1.2, None]) == Array(
        Union({Atomic(int), Atomic(float), Atomic(None)}))
    assert fit([{'a': 1, 'b': 2}, {'a': 1, 'b': 5}]) == Array(
        Record({'a': Atomic(int), 'b': Atomic(int)}))
    assert fit([]) == Array(Unknown())


def test_unify_records():
    assert fit({'1': 1, '2': 2}) == Record(
        {'1': Atomic(int), '2': Atomic(int)})
    assert fit(
        {'1': {'a': 5, 'b': 6}, '2': {'a': 34, 'b': None}}
    ) == UniformRecord(Record({'a': Atomic(int), 'b': Optional(Atomic(int))}))
    assert fit(
        {'1': [{'a': 5, 'b': 6}], '2': [{'a': 34, 'b': None}]}
    ) == UniformRecord(Array(Record({'a': Atomic(int), 'b': Optional(Atomic(int))})))

def test_no_unify_records():
    init.setup(unify_records=False)
    assert fit({'1': 1, '2': 2}) == Record(
        {'1': Atomic(int), '2': Atomic(int)})
    assert fit(
        {'1': {'a': 5, 'b': 6}, '2': {'a': 34, 'b': None}}
    ) == Record({'1': Record({'a': Atomic(int), 'b': Atomic(int)}), '2': Record({'a': Atomic(int), 'b': Atomic(None)})})
    assert fit(
        {'1': [{'a': 5, 'b': 6}], '2': [{'a': 34, 'b': None}]}
    ) == Record({'1': Array(Record({'a': Atomic(int), 'b': Atomic(int)})), '2': Array(Record({'a': Atomic(int), 'b': Atomic(None)}))})