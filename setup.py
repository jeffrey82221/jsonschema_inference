"""
How to sent this package onto PyPi?

1) Building Package Release Tar:
```python setup.py sdist```

2) Upload Package to PyPi:
```pip install twine```
```twine upload dist/*```
"""
import pathlib
import setuptools
from setuptools import find_packages

with open("README.md", "r") as fh:

    long_description = fh.read()


with open(str(pathlib.Path(__file__).parent.absolute()) +
          "/jsonschema_inference/version.py", "r") as fh:
    version = fh.read().split("=")[1].replace("'", "")

setuptools.setup(

    name="jsonschema-inference",

    version=version,

    author="jeffreylin",

    author_email="jeffrey82221@gmail.com",

    description="Fast Json Schema Inference Engine for API and Jsonl",

    long_description=long_description,

    long_description_content_type="text/markdown",

    url="https://github.com/jeffrey82221/jsonschema_inference",

    packages=find_packages(exclude=('tests',)),

    classifiers=[

        "Programming Language :: Python :: 3",

        "License :: OSI Approved :: MIT License",

        "Operating System :: OS Independent",

    ],
    python_requires='>=3.7',
    tests_require=['pytest'],
    install_requires=[
        'execnet',
        'requests',
        'scalable-cuckoo-filter',
        'tqdm',
        'urllib3',
        'autopep8'
    ],
    entry_points={
        'console_scripts': [
            'jsonschema-inference = \
        jsonschema_inference.cmd.inference:run',
        ],
    },
    extras_require={
        'ray': ["ray"]
    }
)
