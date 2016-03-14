import uuid
import adage.graph
from node import Node

def mknode(dag,task, nodename = 'node', depends_on = None):
    dag.addTask(task,nodeobj,depends_on)

def mk_dag():
    return adage.graph.AdageDAG()
