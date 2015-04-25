import adage
from adage import adagetask, rulefunc,mknode,signature,get_node_by_name,result_of
import networkx as nx
import random
import logging
import time
log = logging.getLogger(__name__)

def random_dag(nodes, edges):
    """Generate a random Directed Acyclic Graph (DAG) with a given number of nodes and edges."""
    G = nx.DiGraph()
    for i in range(nodes):
        mknode(G,nodename = 'demo_node_{}'.format(i), sig = hello.s(workdir = 'workdir_{}'.format(i)))
  
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
  if random.random() < 0.3:
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


  rules = []
  rules += [ (nodes_present.s([1]), schedule_after_these.s([1],note = 'depends on one')),
             (nodes_present.s([4,1]), schedule_after_these.s([4,1],note = 'depends on two'))
           ]

  adage.rundag(dag,rules,track = True)

if __name__=='__main__':
  main()