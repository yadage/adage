from setuptools import setup, find_packages

setup(
    name = 'adage',
    author = 'Lukas Heinrich',
    author_email = 'lukas.heinrich@gmail.com',
    version = '0.10.1',
    description = 'running dynamic DAG workflows',
    packages = find_packages(),
    python_requires = '>=3.6',
    install_requires = [
        'networkx>=2.4'
    ],
    extras_require = {
        'develop': [
           'pyflakes',
           'pytest>=3.2.0',
           'pytest-cov>=2.5.1',
           'python-coveralls'
        ],
        'viz' : [
            'pydot',
            'pygraphviz',
            'pydotplus'
        ]
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "License :: OSI Approved :: MIT License",
        "Intended Audience :: Science/Research",
        "Topic :: Scientific/Engineering",
        "Topic :: Scientific/Engineering :: Physics",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: Implementation :: CPython",
    ],
)
