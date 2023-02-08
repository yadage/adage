# Adage - A DAG Executor

[![CI](https://github.com/yadage/adage/actions/workflows/ci.yml/badge.svg)](https://github.com/yadage/adage/actions/workflows/ci.yml?query=branch%3Amain)
[![Code Health](https://landscape.io/github/yadage/adage/main/landscape.svg?style=flat)](https://landscape.io/github/yadage/adage/main)
[![PyPI](https://img.shields.io/pypi/v/adage.svg)](https://pypi.python.org/pypi/adage)
[![Coverage Status](https://coveralls.io/repos/github/yadage/adage/badge.svg?branch=main)](https://coveralls.io/github/yadage/adage?branch=main)
[![Documentation Status](https://readthedocs.org/projects/adage/badge/?version=latest)](http://adage.readthedocs.io/en/latest/?badge=latest)

This is a small experimental package to see how one could describe workflows that are not completely known at definition time. Tasks should be runnable both in a multiprocessing pool, or using a number of celery workers or a IPython cluster.

### Example

![example image](./example_workflow.gif "dynamically extended workflow")



## Problem

Workflows can be comfortably represented by directed acyclic graphs (DAGs). But sometimes the precise structure of the graph is not known before the processing starts. Instead often one only has partial information of what kind of edges are possible and depending on a certain result in a node the DAG might be appended with more nodes and edges.

For example, one node (call it "node A") could be downloading a list of files, which can be processed in parallel. The DAG would therefore have one node for each file-processing (let's call them    "node_file_1" to "node_file_n") depending on "node A". Since the exact number of files is not known until run-time, we cannot map out the DAG beforehand. Also after this "map"-step one might want to have a "reduce"-step to merge the individual result. This can also only be scheduled after the number of "map"-nodes is known.

Another example is that one might have a whole set of nodes that run a certain kind of task (e.g. produce a PDF file). One could imagine wanting to have a "reduce"-type task which merges all these individual PDF files. While any given node does not know where else PDF-generating tasks are scheduled, one can wait until no edges to PDF-generating tasks are possible anymore to then append a PDF-merging node to the DAG.

## Solution

Generically, we want individual nodes to have a limited set of operations they can do on the DAG that they are part of. Specifically we can only allow queries on the structure of the DAG as well as append operations, nodes must not be able to remove nodes. The way we implement this is that we have a append-only record of scheduled rules. A rule is a pair of functions (predicate,body) that operate on the DAG. The predicate is a query function that inspects the graph to decide whether the DAG has enough information to apply the body (e.g. are edges of a certain type still possible to append or not?). If the DAG does have enough information the body which is an append-only operation on the DAG is applied, i.e. nodes are added . Periodically the list of rules is iterated to extend the DAG where possible.

### Rules for Rules

There are a couple of rule that the rules need to obey themselves in order to make 

- it is the responsibility of the predicate to signal that all necessary nodes for the body are present in the DAG. Examples are:
	- wait until    no nodes of a particular type could possibly be added to the DAG. This requires us to know what kind of edges are valid on a global level.
        - wait until a certain number of nodes are present in the DAG (say )
	- select a certain set of nodes by their unique id (useful to attach a sub-DAG to a existing node from within that node)
	
- the only valid edges that you can dynamically add are ones that point away from existing nodes to new nodes.. edges directed *towards* existing nodes would introduce new dependencies which were not present before and so that job might have already run, or be currently running

