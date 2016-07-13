from setuptools import setup, find_packages

setup(
    name = 'adage',
    author = 'Lukas Heinrich',
    author_email = 'lukas.heinrich@gmail.com',
    version = '0.4.0',
    description = 'running dynamic DAG workflows',
    packages = find_packages(),
    install_requires = [
        'networkx',
        'pydot2',
        'pygraphviz',
        'pydotplus'
    ]
)
