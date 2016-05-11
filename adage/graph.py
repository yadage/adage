import networkx as nx
from node import Node
import logging

log = logging.getLogger(__name__)
class AdageDAG(nx.DiGraph):
    def addTask(self,task, nodename = 'node', depends_on = None):
        node = Node(nodename,task)
        self.addNode(node,depends_on)
        return node
    
    def addNode(self,nodeobj,depends_on = None):
        self.add_node(nodeobj.identifier, {'nodeobj': nodeobj})
        for parent in (depends_on or []):
            self.addEdge(parent,nodeobj)
        
    def addEdge(self,fromobj,toobj):
        self.add_edge(fromobj.identifier,toobj.identifier)

    def getNode(self,ident):
        return self.node[ident]['nodeobj']
        
    def getNodeByName(self,name):
        matching = [x for x in self.nodes() if self.getNode(x).name == name]
        if len(matching) > 1:
            log.error('requested name %s',name)
            log.error('matching nodes %s',[self.getNode(x) for x in matching])
            raise RuntimeError("getting node by name resulted in multiple matching nodes, try getting by ID")
        return self.getNode(matching[0]) if matching else None
