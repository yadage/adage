from node import Node
import networkx as nx
import uuid

def get_nodeobj(dag,node):
  return dag.node[node]['nodeobj']

def add_edge(dag,from_node,to_node):
  dag.add_edge(from_node.identifier,to_node.identifier)

def mknode(dag,sig, nodename = 'node', depends_on = []):
  nodeobj = Node(str(uuid.uuid1()),nodename,sig)

  dag.add_node(nodeobj.identifier,nodeobj = nodeobj)
  for parent in depends_on:
    add_edge(dag,parent,nodeobj)
  return nodeobj

def mk_dag():
  return nx.DiGraph()
