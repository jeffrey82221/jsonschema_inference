import setuptools
from setuptools import find_packages

with open("README.md", "r") as fh:

    long_description = fh.read()

setuptools.setup(

    name="jsonschema-inference",  # Replace with your username

    version="0.0.1",

    author="jeffreylin",

    author_email="jeffrey82221@gmail.com",

    description="Fast Json Schema Inference Engine for API and Jsonl",

    long_description=long_description,

    long_description_content_type="text/markdown",

    url="https://github.com/jeffrey82221/jsonschema_inference",

    packages=find_packages(exclude=('tests',)),

    package_dir={'': 'src'},

    classifiers=[

        "Programming Language :: Python :: 3",

        "License :: OSI Approved :: MIT License",

        "Operating System :: OS Independent",

    ],
    python_requires='>=3.7',
    tests_require=['pytest'],
    install_requires=[
        'execnet',
        'ray',
        'requests',
        'scalable-cuckoo-filter',
        'tqdm',
        'urllib3'
    ]
)
