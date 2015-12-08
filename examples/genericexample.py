import adage
from adage import adagetask, rulefunc,mknode
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
    
    nodecache = {}
    for i in range(nodes):
        nodecache[i] = mknode(G,nodename = 'demo_node_{}'.format(i), sig = hello.s(workdir = 'workdir_{}'.format(i)))
  
    while edges > 0:
        a = random.randint(0,nodes-1)
        b=a
        while b==a:
            b = random.randint(0,nodes-1)
        G.add_edge(nodecache[a].identifier,nodecache[b].identifier)
        if nx.is_directed_acyclic_graph(G):
            edges -= 1
        else:
            # we closed a loop!
            G.remove_edge(nodecache[a].identifier,nodecache[b].identifier)
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


@rulefunc
def nodes_present(nodenrs,dag):
  return all(n in dag.nodes() for n in nodenrs)

@rulefunc
def schedule_after_these(parentnrs,note,dag):
  newnode = mknode(dag,nodename = 'dynamic_node',sig = newtask.s(note = note))
  for parent in parentnrs:
    dag.add_edge(parent,newnode['nodenr'])

def main():
  dag = random_dag(6,5)

  logging.basicConfig(level = logging.DEBUG)

  rules = []
  rules += [ (nodes_present.s([1]), schedule_after_these.s([1],note = 'depends on one')),
             (nodes_present.s([4,1]), schedule_after_these.s([4,1],note = 'depends on two'))
           ]

  adage.rundag(dag,rules,track = True, workdir = 'workdirtrack', trackevery = 4)

if __name__=='__main__':
  main()