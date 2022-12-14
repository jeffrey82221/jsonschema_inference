"""
Json Schema Objects
that supports union operations

TODO:
- [X] Set operation of JsonSchema(s) (defined as staticmethod of Union)
    - converting a list of JsonSchemas into one schema.
- [X] UniformDict build
- [X] A SchemaFitter that infer Schema from Json(s)
- [X] Consider empty list in schema objs / fitter
- [X] Add DynamicDict object to represent dictionary with changing of keys
- [X] Add content counting data content to DynamicDict as addition to ._content
    (
        1. We can count the occurrence of key.
        2. We can represent the objects as tuple appearance count.
            e.g., DynamicDict[{'apple' (120): str, 'banana' (123): str, ..., 'car' (1): str}]
        3. Allow counter to be added together
    )
- [ ] Allow another DynamicDict to capture mandatory fields & co-occurrence relationship between fields
- [ ] Enable representing the __init__ of DynamicDict (same as other schema types)
"""
import abc
import copy
import typing
from functools import reduce
from collections import Counter
__all__ = [
    'Simple',
    'List',
    'Union',
    'Dict',
    'Optional',
    'UniformDict',
    'Unknown',
    'DynamicDict'
]


class JsonSchema:
    def __init__(self, content=None):
        self._content = content
        self.check_content()

    @abc.abstractmethod
    def check_content(self):
        raise NotImplementedError

    def __eq__(self, e):
        if isinstance(self, type(e)):
            return self._content == e._content
        else:
            return False

    def __or__(self, e):
        return self._base_or(e)

    def _base_or(self, e):
        old = copy.deepcopy(self)
        new = copy.deepcopy(e)
        if old == new or isinstance(e, Unknown):
            return old
        else:
            if old._content is None:
                if new._content is None:
                    return old
                else:
                    if isinstance(new, Optional):
                        return new
                    else:
                        return Optional(new)
            elif new._content is None:
                return Optional(old)
            elif isinstance(new, Union):
                new |= old
                return new
            else:
                return Union({old, new})

    def _base_hash(self):
        return hash(self._content)

    def __hash__(self):
        return self._base_hash()


class Simple(JsonSchema):
    """
    simple json units, such as `int`, `float`, `null`,
        `str`, etc.
    """

    def __init__(self, content: type):
        super().__init__(content)

    def check_content(self):
        assert isinstance(
            self._content, type) or self._content is None, 'Simple content should be `int`, `str`, `float`, or `None`'

    def __repr__(self):
        if self._content is None:
            return 'Simple(None)'
        else:
            return f'Simple({self._content.__name__})'


class List(JsonSchema):
    def __init__(self, content: JsonSchema):
        super().__init__(content)

    def check_content(self):
        assert isinstance(
            self._content, JsonSchema), 'List content should be JsonSchema'

    def __repr__(self):
        return f'List({self._content})'

    def __or__(self, e):
        if isinstance(e, List):
            new = copy.deepcopy(e)
            old = copy.deepcopy(self)
            return List(old._content | new._content)
        else:
            return self._base_or(e)


class Unknown(JsonSchema):
    def check_content(self):
        pass

    def __or__(self, e):
        return e

    def __repr__(self):
        return 'Unknown()'


class Union(JsonSchema):
    @staticmethod
    def set(json_schemas: typing.Set[JsonSchema]) -> JsonSchema:
        if json_schemas:
            result = reduce(lambda a, b: a | b, json_schemas)
        else:
            result = Unknown()
        return result

    def __init__(self, content: typing.Set[JsonSchema]):
        super().__init__(content)

    def check_content(self):
        assert isinstance(self._content, set), 'Union content should be set'
        for e in self._content:
            assert isinstance(
                e, JsonSchema), 'Union content elements should be JsonSchema'

    def __repr__(self):
        content_str = ','.join(map(str, list(self._content)))
        content_str = '{' + content_str + '}'
        return f'Union({content_str})'

    def __or__(self, e):
        new = copy.deepcopy(e)
        if isinstance(new, Union):
            new_set = copy.deepcopy(self._content)
            for element in new._content:
                new_set |= element
            return Union(new_set)
        else:
            new_set = copy.deepcopy(self._content)
            new_set.add(new)
            return Union(new_set)

    def __hash__(self):
        return hash(tuple(self._content))


class Optional(Union):
    def __init__(self, content: JsonSchema):
        self._the_content = content
        super().__init__({Simple(None), content})

    def __repr__(self):
        return f'Optional({self._the_content})'

    def __or__(self, e: JsonSchema):
        old = copy.deepcopy(self)
        new = copy.deepcopy(e)
        if new._content is None or old == new or old._the_content == new:
            return old
        else:
            # add element in new to the orignal element in OptionalUnion
            if isinstance(new, Union):
                if isinstance(new, Optional):
                    result = Optional(old._the_content | new._the_content)
                else:
                    result = Optional(old._the_content | new)
            else:
                result = Optional(old._the_content | new)
            return result


class Dict(JsonSchema):
    def __init__(self, content: dict):
        super().__init__(content)

    def check_content(self):
        assert isinstance(self._content, dict), 'Dict content should be dict'
        for key in self._content:
            assert isinstance(key, str), 'Dict content key should be str'
            assert isinstance(
                self._content[key], JsonSchema), 'Dict content value should be JsonSchema'

    def __repr__(self):
        return f'Dict({self._content})'

    def __hash__(self):
        return hash(tuple(sorted(self._content.items())))

    def __or__(self, e):
        if isinstance(e, DynamicDict):
            return e | self
        elif isinstance(e, Dict):
            old = copy.deepcopy(self)
            new = copy.deepcopy(e)
            if old._content.keys() == new._content.keys():
                for key in old._content:
                    old._content[key] |= new._content[key]
                return old
            else:
                result_dict = {}
                key_counter = Counter()
                for key in set(list(old._content.keys()) +
                               list(new._content.keys())):
                    if key in old._content and key in new._content:
                        result_dict[key] = old._content[key] | new._content[key]
                    elif key in old._content:
                        result_dict[key] = old._content[key]
                    elif key in new._content:
                        result_dict[key] = new._content[key]
                for key in old._content.keys():
                    key_counter[key] += 1
                for key in new._content.keys():
                    key_counter[key] += 1
                return DynamicDict(result_dict, key_counter)
        else:
            return self._base_or(e)

    def to_uniform_dict(self):
        schemas = [v for v in self._content.values()]
        uniform_content = Union.set(schemas)
        return UniformDict(uniform_content)


class DynamicDict(Dict):
    """
    Dictionary where keys are not strict
    (some keys can be optional)
    """

    def __init__(self, content: dict, key_counter):
        super().__init__(content)
        self._key_counter = key_counter

    def __repr__(self):
        content_strs = []
        for key, cnt in self._key_counter.most_common(10):
            content_strs.append(f'("{key}", {cnt}): {self._content[key]}')
        content_whole_str = ','.join(content_strs)
        content_whole_str = '{' + content_whole_str + '}'
        return f'DynamicDict({content_whole_str})'

    def __hash__(self):
        return hash(tuple(sorted(self._content.items()))) + \
            hash(tuple(sorted(self._key_counter.items())))

    def __or__(self, e):
        result_dict = dict()
        if isinstance(e, DynamicDict):
            old = copy.deepcopy(self)
            new = copy.deepcopy(e)
            for key in set(list(old._content.keys()) +
                           list(new._content.keys())):
                if key in old._content and key in new._content:
                    result_dict[key] = old._content[key] | new._content[key]
                elif key in old._content:
                    result_dict[key] = old._content[key]
                elif key in new._content:
                    result_dict[key] = new._content[key]
            return DynamicDict(
                result_dict, old._key_counter + new._key_counter)
        elif isinstance(e, Dict):
            old = copy.deepcopy(self)
            new = copy.deepcopy(e)
            for key in set(list(old._content.keys()) +
                           list(new._content.keys())):
                if key in old._content and key in new._content:
                    result_dict[key] = old._content[key] | new._content[key]
                elif key in old._content:
                    result_dict[key] = old._content[key]
                elif key in new._content:
                    result_dict[key] = new._content[key]
            for key in new._content.keys():
                old._key_counter[key] += 1
            return DynamicDict(result_dict, old._key_counter)
        else:
            return self._base_or(e)


class UniformDict(JsonSchema):
    """
    Dictionary where value elements
    are united into a JsonSchema.
    """

    def __init__(self, content: JsonSchema):
        super().__init__(content)

    def check_content(self):
        assert isinstance(
            self._content, JsonSchema), f'UniformDict content should be JsonSchema, but it is {self._content}'

    def __repr__(self):
        return f'UniformDict({self._content})'

    def __or__(self, e):
        if isinstance(e, UniformDict):
            new = copy.deepcopy(e)
            old = copy.deepcopy(self)
            return UniformDict(old._content | new._content)
        else:
            return self._base_or(e)
