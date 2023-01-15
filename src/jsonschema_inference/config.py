"""
Config the Json Schema Inference Model
"""


class Config:
    """
    - 1. equivalence model: kind | label
        - kind: Turn on DynamicRecord for unifing Records with different fields
        - label: Turn off DynamicRecord and construct Union of Records with inconsistent fields
    - 2. enable_uniform_record: True | False (whether try to unify Record values for Record with values of same kind)
    """

    def __init__(self, unify_records=True, equivalence_mode='kind'):
        self.init(
            unify_records=unify_records,
            equivalence_mode=equivalence_mode)

    def init(self, unify_records=True, equivalence_mode='kind'):
        self._unify_records = unify_records
        assert equivalence_mode == 'kind' or equivalence_mode == 'label'
        self._equivalence_mode = equivalence_mode

    @property
    def unify_records(self) -> bool:
        return self._unify_records

    @property
    def equivalence_mode(self) -> str:
        return self._equivalence_mode


config = Config()
init = config.init
