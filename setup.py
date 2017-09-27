from setuptools import setup, find_packages

setup(
    name = 'adage',
    author = 'Lukas Heinrich',
    author_email = 'lukas.heinrich@gmail.com',
    version = '0.8.2',
    description = 'running dynamic DAG workflows',
    packages = find_packages(),
    install_requires = [
        'networkx==1.11'
    ],
    extras_require = {
        'viz' : [
            'pydot2',
            'pygraphviz',
            'pydotplus'
        ]
    },
)
