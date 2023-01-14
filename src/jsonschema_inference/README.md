# How to compile `schema` package?

```
python -m nuitka --module --include-package=schema schema --python-flag=nosite,-O --prefer-source-code \
    --clang --plugin-enable=anti-bloat,implicit-imports,data-files,pylint-warnings
```
# Paper Study `Parametric Schema Inference for Massive JSON Datasets`

## How the paper define the json schema ? 

- [X] Atomic Values (aka. `Simple`):
	- numbers (aka. `int`, `float`)
	- strings (aka. `str`)
	- booleans (aka. `bool`)
	- null (aka. `None`)
- Complex Values:
	- record: unordered sets of key/value paris (aka. `Dict`)
	- array: ordered list of values (aka. `List`)

## Additional Union Object for expersiveness

union are represented by `+` and `*` symbol:
- Union of two types: `type1 + type2`. (aka. `Union({type1, type2})`
- Union of a non-null type and a null value: `type1 + null`. (aka. `Optional(non_null_type)`

## How the paper merge schemas? 

- Two atomic types: type1 | type2 -> type1 + type2
- Two record types: missing keys are documented as Optional. 


# TODO:

- [ ] move `try_unify_dict` from `inference_base` to `schema_fitter` and add a unify_dict bool arg to `fit`. 
- [ ] Enable the selection of kind equivalence and label equivalence:
	- [ ] kind equivalence: 
		-  [ ] Turn on `DynamicDict`
		-  [ ] Allow `DynamicDict` to be represented with marking of whether a field is optional (missing some times) or not. 
	-  [ ] label equivalence:
		-  [ ] Turn off `DynamicDict` (Dict with different labels should be merged into `Union` of Dict(s))
- [ ] Construction of UniformDict should be optional, too. 
- [X] Try to imitate the json schema of the paper
	- [X] `Simple` -> `Atomic`
	- [X] `Dict` -> `Record`
	- [X] `List` -> `Array`
- [ ] Allow passing of `equavalence_model = 'kind' | 'label'` and `unify_duct = True | False` from `InferenceEngine` (also `APIInferenceEngine & JsonlInferenceEngine`
- [ ] Extract a `SpeedInferenceEngine` for `APIInfernceEngine` & `JsonlInferenceEngine`. 
- [ ] `ViewDataClass` for Jsonl:
- [ ] A `JsonlPandasAdaptor` for converting jsonl file to pandas dataframe, where a list of keys can be provided and mapped to a column of a pandas table. (The `overview` dataclass simply convert jsonl to pandas.DataFrame.) 
- [ ] A column mapping function can be provided to the class to transform the column. 