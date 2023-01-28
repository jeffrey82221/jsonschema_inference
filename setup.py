import setuptools

with open("README.md", "r") as fh:

    long_description = fh.read()

setuptools.setup(

    name="jsonschema-inference",  # Replace with your username

    version="0.0.0",

    author="jeffreylin",

    author_email="jeffrey82221@gmail.com",

    description="Fast Json Schema Inference Engine for API and Jsonl",

    long_description=long_description,

    long_description_content_type="text/markdown",

    url="https://github.com/jeffrey82221/jsonschema_inference",

    packages=['jsonschema_inference'],

    package_dir={'': 'src'},

    classifiers=[

        "Programming Language :: Python :: 3",

        "License :: OSI Approved :: MIT License",

        "Operating System :: OS Independent",

    ],
    python_requires='>=3.7',
    tests_require=['pytest'],
    install_requires=[
        'execnet==1.9.0',
        'ray==2.2.0',
        'requests==2.28.2',
        'scalable-cuckoo-filter==1.1',
        'tqdm==4.64.1',
        'urllib3==1.26.14'
    ]
)
