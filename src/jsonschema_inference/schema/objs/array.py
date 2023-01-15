"""
Array Json Schema Objects

e.g., [1,2,3,'apple']
"""
import copy
from .basic import JsonSchema
__all__ = [
    'Array'
]


class Array(JsonSchema):
    def __init__(self, content: JsonSchema):
        super().__init__(content)

    def check_content(self):
        assert isinstance(
            self._content, JsonSchema), 'Array content should be JsonSchema'

    def __repr__(self):
        return f'Array({self._content})'

    def __or__(self, e):
        if isinstance(e, Array):
            new = copy.deepcopy(e)
            old = copy.deepcopy(self)
            return Array(old._content | new._content)
        else:
            return self._base_or(e)
