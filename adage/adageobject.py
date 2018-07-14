import adage.graph

class adageobject(object):
    def __init__(self,dag = None, rules = None, applied_rules = None):
        self.dag = dag or adage.graph.AdageDAG()
        self.rules = rules or []
        self.applied_rules = applied_rules or []
