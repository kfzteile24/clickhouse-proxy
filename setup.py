from setuptools import setup, find_packages

def get_long_description():
    with open('README.md') as f:
        return f.read()

setup(
    name='clickhouse-proxy',
    version='0.3.2',

    description="HTTP proxy for ODBC queries for ClickHouse",
    long_description=get_long_description(),
    long_description_content_type='text/markdown',

    install_requires=[
        'pyyaml',
        'requests',
        'sqlparse',
        'uvicorn'
    ],

    dependency_links=[
    ],

    packages=find_packages(),

    author='kfzteile24 GmbH',
    license='MIT',

    entry_points={
        'console_scripts': [
            'clickhouse-proxy = clickhouse_proxy.main:main',
        ],
    },

)
