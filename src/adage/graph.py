import networkx as nx
import logging

from .node import Node

log = logging.getLogger(__name__)
class AdageDAG(nx.DiGraph):
    def addTask(self,task, nodename = 'node', depends_on = None):
        '''
        add a node based on a task to the DAG
        :param: task: the task object
        :param: nodename: name of the node
        :param: depends_on: list of node objects to be declared as dependencies
        :return: the newly created node object
        '''
        node = Node(nodename,task)
        self.addNode(node,depends_on)
        return node

    def addNode(self,nodeobj,depends_on = None):
        self.add_node(nodeobj.identifier, nodeobj=nodeobj)
        for parent in (depends_on or []):
            self.addEdge(parent,nodeobj)

    def removeNode(self,nodeobj):
        self.remove_node(nodeobj.identifier)

    def addEdge(self,fromobj,toobj):
        self.add_edge(fromobj.identifier,toobj.identifier)

    def getNode(self,ident):
        return self.nodes[ident]['nodeobj']

    def getNodeByName(self,name, nodefilter = None):
        nodefilter = nodefilter or (lambda x: True)
        matching = [x for x in self.nodes() if self.getNode(x).name == name and nodefilter(self.getNode(x))]
        if len(matching) > 1:
            log.error('requested name %s',name)
            log.error('matching nodes %s',[self.getNode(x) for x in matching])
            raise RuntimeError("getting node by name resulted in multiple matching nodes, try getting by ID")
        return self.getNode(matching[0]) if matching else None
