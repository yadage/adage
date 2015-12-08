import adage
import adage.dagutils
from adage import adagetask, functorize, Rule,mknode, mk_dag
import networkx as nx
import random
import logging
import time
log = logging.getLogger(__name__)


# random DAG code taken from IPython cluster doc
# http://ipython.org/ipython-doc/dev/parallel/dag_dependencies.html
def random_dag(nodes, edges):
    """Generate a random Directed Acyclic Graph (DAG) with a given number of nodes and edges."""
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
  log.info("running job in workdir {}".format(workdir))
  time.sleep(2+5*random.random())
  if random.random() < 0.001:
    log.error('ERROR! in workdir {}'.format(workdir))
    raise IOError
  log.info("done {}".format(workdir))

@adagetask
def newtask(note):
  log.info('doing some other task this is our note: {}'.format(note))
  time.sleep(2+5*random.random())


@functorize
def nodes_present(nodenrs,dag):
  names = ['demo_node_{}'.format(i) for i in nodenrs]
  return all(dag.getNodeByName(name) for name in names)

@functorize
def schedule_after_these(parentnrs,note,dag):
  names = ['demo_node_{}'.format(i) for i in parentnrs]
  nodes = [dag.getNodeByName(name) for name in names]

  newnode = mknode(dag,nodename = 'dynamic_node',sig = newtask.s(note = note))
  for parentnode in nodes:
    adage.dagutils.add_edge(dag,parentnode,newnode)

def main():
  dag = random_dag(6,5)

  adage_dag = mk_dag()
  numbered = {}
  for i,node in enumerate(dag.nodes()):
    numbered[node] = mknode(adage_dag,nodename = 'demo_node_{}'.format(node), sig = hello.s(workdir = 'workdir_{}'.format(node)))
  for i,node in enumerate(dag.nodes()):
    print 'pre for: {} are: {}'.format(node,dag.predecessors(node))
    for parent in dag.predecessors(node):
      adage_dag.addEdge(numbered[parent],numbered[node])
  

  logging.basicConfig(level = logging.DEBUG)

  rules = []
  rules += [ Rule(nodes_present.s([1]), schedule_after_these.s([1],note = 'depends on one')),
             Rule(nodes_present.s([4,1]), schedule_after_these.s([4,1],note = 'depends on two'))
           ]

  adage.rundag(adage_dag,rules,track = True, workdir = 'workdirtrack', trackevery = 4)

if __name__=='__main__':
  main()