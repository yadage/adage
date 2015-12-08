from node import Node
import networkx as nx
import uuid
import adage.graph

def add_edge(dag,from_node,to_node):
  dag.addEdge(from_node,to_node)

def mknode(dag,task, nodename = 'node', depends_on = []):
  nodeobj = Node(str(uuid.uuid1()),nodename,task)

  dag.addNode(nodeobj)
  for parent in depends_on:
    dag.addEdge(parent,nodeobj)
  return nodeobj

def mk_dag():
  return adage.graph.AdageDAG()
