from setuptools import setup

extras_require = {
    "develop": ["pyflakes", "pytest>=3.2.0", "pytest-cov>=2.5.1", "python-coveralls"],
    "viz": [
        "pydot",
        "pygraphviz!=1.8",  # c.f. https://github.com/pygraphviz/pygraphviz/issues/395
        "pydotplus",
    ],
}

setup(extras_require=extras_require)
