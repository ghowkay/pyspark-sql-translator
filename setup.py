from setuptools import setup, find_packages

setup(
    name='pyspark_sql_translator',
    version='0.1.0',
    packages=find_packages(),
    install_requires=[
        'pyspark>=x.x.x'  # Specify the appropriate version
    ],
    # ... other metadata ...
)