"""
Basic Json Schema Objects
"""
import abc
import copy
import typing
from functools import reduce
__all__ = [
    'Atomic',
    'Union',
    'Optional',
    'Unknown'
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


class Atomic(JsonSchema):
    """
    simple json units, such as `int`, `float`, `null`,
        `str`, etc.
    """

    def __init__(self, content: typing.Union[type, None]):
        super().__init__(content)

    def check_content(self):
        assert isinstance(
            self._content, type) or self._content is None, 'Atomic content should be `int`, `str`, `float`, or `None`'

    def __repr__(self):
        if self._content is None:
            return 'Atomic(None)'
        else:
            return f'Atomic({self._content.__name__})'


class Unknown(JsonSchema):
    def check_content(self):
        pass

    def __or__(self, e):
        return e

    def __repr__(self):
        return 'Unknown()'


class Union(JsonSchema):
    @staticmethod
    def set(json_schemas: typing.Iterable[JsonSchema]) -> JsonSchema:
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
        content_str = ', '.join(map(str, list(self._content)))
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
        super().__init__({Atomic(None), content})

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
