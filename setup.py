from setuptools import setup, find_packages

setup(
  name = 'adage',
  author = 'Lukas Heinrich',
  version = '0.1.1',
  packages = find_packages(),
  install_requires = [
    'networkx',
    'pygraphviz'
  ]
)
