from setuptools import setup, find_packages

setup(
    name = 'adage',
    author = 'Lukas Heinrich',
    author_email = 'lukas.heinrich@gmail.com',
    version = '0.8.7',
    description = 'running dynamic DAG workflows',
    packages = find_packages(),
    install_requires = [
        'networkx==1.11'
    ],
    extras_require = {
        'develop': [
           'pyflakes',
           'pytest>=3.2.0',
           'pytest-cov>=2.5.1',
           'python-coveralls'
        ],
        'viz' : [
            'pydot2',
            'pygraphviz',
            'pydotplus'
        ]
    },
)
