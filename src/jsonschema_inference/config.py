"""
Config the Json Schema Inference Model
- 1. equivalence model: kind | label 
    - kind: Turn on DynamicRecord for unifing Records with different fields
    - label: Turn off DynamicRecord and construct Union of Records with inconsistent fields
- 2. enable_uniform_record: True | False (whether try to unify Record values for Record with values of same kind)
"""
class Config:
    def __init__(self, unify_records = True):
        self.setup(unify_records=unify_records)

    def setup(self, unify_records = True):
        self._unify_records = unify_records

    @property
    def unify_records(self) -> bool:
        return self._unify_records



config = Config()
