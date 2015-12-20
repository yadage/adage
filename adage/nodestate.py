class NodeState(object):
    def __init__(self,name):
        self.name = name
    def __repr__(self):
        return '<NodeState: {}>'.format(self.name)
    def __str__(self):
        return self.name
    
DEFINED     = NodeState('DEFINED')
RUNNING     = NodeState('RUNNING')
FAILED      = NodeState('FAILED')
SUCCESS     = NodeState('SUCCESS')