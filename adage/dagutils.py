import adage.graph

def mknode(dag,task, nodename = 'node', depends_on = None):
    dag.addTask(task,nodename,depends_on)

def mk_dag():
    return adage.graph.AdageDAG()
