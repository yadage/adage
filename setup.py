from setuptools import setup

extras_require = {
    "develop": ["pyflakes", "pytest>=3.2.0", "pytest-cov>=2.5.1", "python-coveralls"],
    "viz": [
        "pydot>=1.2.3",  # c.f. https://github.com/networkx/networkx/pull/2272
        "pygraphviz>=1.0,!=1.8",  # c.f. https://github.com/pygraphviz/pygraphviz/issues/395
    ],
}

setup(extras_require=extras_require)
