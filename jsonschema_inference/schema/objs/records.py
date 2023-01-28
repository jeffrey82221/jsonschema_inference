"""
Record Json Schema Objects

TODO:
- [X] Allow another DynamicDict to capture mandatory fields & co-occurrence relationship between fields
- [X] Enable representing the __init__ of DynamicDict (same as other schema types)
"""
from __future__ import annotations
import copy
from collections import Counter
from ...config import config
from .basic import JsonSchema, Union
__all__ = [
    'Record',
    'UniformRecord',
    'DynamicRecord'
]


class Record(JsonSchema):
    def __init__(self, content: dict):
        super().__init__(content)

    def check_content(self):
        assert isinstance(self._content, dict), 'Record content should be dict'
        for key in self._content:
            assert isinstance(key, str), 'Record content key should be str'
            assert isinstance(
                self._content[key], JsonSchema), 'Record content value should be JsonSchema'

    def __repr__(self):
        return f'Record({self._content})'

    def __hash__(self):
        return hash(tuple(sorted(self._content.items())))

    def __or__(self, e):
        if isinstance(e, DynamicRecord):
            return e | self
        elif isinstance(e, Record):
            old = copy.deepcopy(self)
            new = copy.deepcopy(e)
            if old._content.keys() == new._content.keys():
                old = Record.merge_label_equal_fields(old, new)
                return old
            else:
                if config.equivalence_mode == 'kind':
                    return DynamicRecord.merge_records_as_dynamic_record(
                        old, new)
                elif config.equivalence_mode == 'label':
                    return self._base_or(e)
        else:
            return self._base_or(e)

    @staticmethod
    def merge_label_equal_fields(old: Record, new: Record):
        for key in old._content:
            old._content[key] |= new._content[key]
        return old

    def to_uniform_dict(self):
        schemas = [v for v in self._content.values()]
        uniform_content = Union.set(schemas)
        return UniformRecord(uniform_content)


class DynamicRecord(Record):
    """
    Dictionary where keys are not strict
    (some keys can be optional)
    """

    def __init__(self, content: dict, key_counter):
        super().__init__(content)
        self._key_counter = key_counter

    def __repr__(self):
        return f'DynamicRecord({self._content}, {self._key_counter})'

    def __hash__(self):
        return hash(tuple(sorted(self._content.items()))) + \
            hash(tuple(sorted(self._key_counter.items())))

    def __or__(self, e):
        if isinstance(e, DynamicRecord):
            old = copy.deepcopy(self)
            new = copy.deepcopy(e)
            return DynamicRecord.merge_dynamic_records(old, new)
        elif isinstance(e, Record):
            old = copy.deepcopy(self)
            new = copy.deepcopy(e)
            return DynamicRecord.merge_dynamic_n_normal_records(old, new)
        else:
            return self._base_or(e)

    @staticmethod
    def merge_dynamic_n_normal_records(old: DynamicRecord, new: Record):
        result_dict = DynamicRecord.__merge_common_fields(old, new)
        for key in new._content.keys():
            old._key_counter[key] += 1
        return DynamicRecord(result_dict, old._key_counter)

    @staticmethod
    def merge_dynamic_records(old: DynamicRecord, new: DynamicRecord):
        result_dict = DynamicRecord.__merge_common_fields(old, new)
        return DynamicRecord(
            result_dict, old._key_counter + new._key_counter)

    @staticmethod
    def merge_records_as_dynamic_record(old: Record, new: Record):
        key_counter: Counter = Counter()
        result_dict = DynamicRecord.__merge_common_fields(old, new)
        for key in old._content.keys():
            key_counter[key] += 1
        for key in new._content.keys():
            key_counter[key] += 1
        return DynamicRecord(result_dict, key_counter)

    @staticmethod
    def __merge_common_fields(old: Record, new: Record):
        result_dict = {}
        old_keys = list(old._content.keys())
        new_keys = list(new._content.keys())
        all_keys = set(old_keys + new_keys)
        for key in all_keys:
            if key in old._content and key in new._content:
                result_dict[key] = old._content[key] | new._content[key]
            elif key in old._content:
                result_dict[key] = old._content[key]
            elif key in new._content:
                result_dict[key] = new._content[key]
        return result_dict


class UniformRecord(JsonSchema):
    """
    Dictionary where value elements
    are united into a JsonSchema.
    """

    def __init__(self, content: JsonSchema):
        super().__init__(content)

    def check_content(self):
        assert isinstance(
            self._content, JsonSchema), f'UniformRecord content should be JsonSchema, but it is {self._content}'

    def __repr__(self):
        return f'UniformRecord({self._content})'

    def __or__(self, e):
        if isinstance(e, UniformRecord):
            new = copy.deepcopy(e)
            old = copy.deepcopy(self)
            return UniformRecord(old._content | new._content)
        else:
            return self._base_or(e)
