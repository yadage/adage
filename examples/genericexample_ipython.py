import adage
from adage import adagetask, adageop, Rule
import networkx as nx

# random DAG code taken from IPython cluster doc
# http://ipython.org/ipython-doc/dev/parallel/dag_dependencies.html
def random_dag(nodes, edges):
    """Generate a random Directed Acyclic Graph (DAG) with a given number of nodes and edges."""
    import random
    G = nx.DiGraph()
    for i in range(nodes):
        G.add_node(i)
    while edges > 0:
        a = random.randint(0,nodes-1)
        b=a
        while b==a:
            b = random.randint(0,nodes-1)
        G.add_edge(a,b)
        if nx.is_directed_acyclic_graph(G):
            edges -= 1
        else:
            # we closed a loop!
            G.remove_edge(a,b)
    return G

@adagetask
def hello(workdir):
    import logging
    logging.basicConfig()
    log = logging.getLogger(__name__)
    import time
    import random
    log.info("running job in workdir %s",workdir)
    time.sleep(2+5*random.random())
    # if random.random() < 0.91:
    #     log.error('ERROR! in workdir %s',workdir)
    #     raise IOError
    log.info("done %s",workdir)

@adagetask
def newtask(note):
    import logging
    logging.basicConfig()
    log = logging.getLogger(__name__)
    import time
    import random
    log.info('doing some other task this is our note: %s',note)
    time.sleep(2+5*random.random())


@adageop
def nodes_present(nodenrs,adageobj):
    names = ['demo_node_{}'.format(i) for i in nodenrs]
    nodes = [adageobj.dag.getNodeByName(name) for name in names]
    return all([node.ready() if node else False for node in nodes])

@adageop
def schedule_after_these(parentnrs,note,tag,adageobj):
    names = ['demo_node_{}'.format(i) for i in parentnrs]
    nodes = [adageobj.dag.getNodeByName(name) for name in names]

    newnode = adageobj.dag.addTask(newtask.s(note = note), nodename = 'demo_node_dynamic_{}'.format(tag))
    for parentnode in nodes:
        adageobj.dag.addEdge(parentnode,newnode)

def main():
    dag = random_dag(5,3)

    adageobj = adage.adageobject()
    numbered = {}
    for node in dag.nodes():
        numbered[node] = adageobj.dag.addTask(hello.s(workdir = 'workdir_{}'.format(node)), nodename = 'demo_node_{}'.format(node))
    for i,node in enumerate(dag.nodes()):
        print 'pre for: {} are: {}'.format(node,dag.predecessors(node))
        for parent in dag.predecessors(node):
            adageobj.dag.addEdge(numbered[parent],numbered[node])


    rules = []
    rules += [
                Rule(nodes_present.s([1]), schedule_after_these.s([1],note = 'depends on one', tag = 'dyn1')),
                Rule(nodes_present.s([4,'dynamic_dyn1']), schedule_after_these.s([4,'dynamic_dyn1'],note = 'depends on two', tag = 'dyn2'))
             ]

    from adage.backends import IPythonParallelBackend
    from ipyparallel import Client
    backend = IPythonParallelBackend(Client(), resolve_like_partial = True)
    adageobj.rules = rules
    adage.rundag(adageobj, backend = backend, default_trackers = True, workdir = 'workdirtrack', trackevery = 4)

if __name__=='__main__':
    main()
