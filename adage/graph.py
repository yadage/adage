import networkx as nx
class AdageDAG(nx.DiGraph):
    def addNode(self,nodeobj):
        self.add_node(nodeobj.identifier, {'nodeobj': nodeobj})
    def addEdge(self,fromobj,toobj):
        self.add_edge(fromobj.identifier,toobj.identifier)
    def getNode(self,ident):
        return self.node[ident]['nodeobj']
    def getNodeByName(self,name):
        matching = [x for x in self.nodes() if self.getNode(x).name == name]
        return self.getNode(matching[0]) if (len(matching) == 1) else None
        