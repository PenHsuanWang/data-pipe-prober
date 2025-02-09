from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name='data-pipe-prober',
    version='0.0.1',
    description='A CLI tool for comparing data sources using pandas.',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/PenHsuanWang/data-pipe-prober.git",
    author='Ben Wang',
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    python_requires=">=3.7",
    install_requires=[
        "pandas>=1.0.0",
        "pyspark>=3.0.0",  # If you want Spark as a requirement
        # Add other dependencies if needed, e.g. snowflake-connector-python
    ],
    entry_points={
        "console_scripts": [
            "data-pipe-prober=cli:main",  # If "cli.py" is top-level in src/
            # OR "data-pipe-prober=src.cli:main" if "cli.py" is inside "src" package
            # see note below
        ]
    },
)
