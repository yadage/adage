.. Adage documentation master file, created by
   sphinx-quickstart on Tue Feb 21 12:31:20 2017.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to Adage's documentation!
=================================

Contents:

.. toctree::
   :maxdepth: 2

   coderef


Introduction
==================

Adage is a simple run-time that steers the execution of workflows expressed as directed acyclic graphs (DAGs). An important feature is that adage does not assume that the entire graph topology is known in advance, but instead allows a run-time extension of the DAGs.

Basic Concepts
==================

The adage workflow is defined by two main data constructs

- The Current Workflow Graph

  This data structure hold the main information of the *current* workflow. That is nodes representing individual tasks, and edges representing dependency relations

- Extension Rule set

  This data structure holds information of when and how to extend the current DAG by adding new nodes and edges. Each rule defines its own dependencies, and declares by inspection of the current DAG when it may be able to be applied


Task Backends
=================

Each node in the workflow graph is associated with a computing task. This task maybe be processsed by a variety of backends. Oftentimes, task are presented conveniently as Python callables, in which case they may be processed by backends such as Celery, IPython clusters, or a simple multiprocessing pool.

If needed custom task objects may be attached to the nodes and a suitable backend may be plugged into the run-time.

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

