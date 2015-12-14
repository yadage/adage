import uuid
import adage.graph
from node import Node

def mknode(dag,task, nodename = 'node', depends_on = None):
    nodeobj = Node(str(uuid.uuid1()),nodename,task)

    dag.addNode(nodeobj)

    if not depends_on: return nodeobj

    for parent in depends_on:
        dag.addEdge(parent,nodeobj)
    return nodeobj

def mk_dag():
    return adage.graph.AdageDAG()
