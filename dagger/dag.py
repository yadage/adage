import multiprocessing
import time
import random
import networkx as nx
import subprocess

import logging
FORMAT = '%(asctime)s %(message)s'
logging.basicConfig(format=FORMAT,level=logging.INFO)

log = logging.getLogger(__name__)

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

def hello(workdir):
  log.info("running job in workdir {}".format(workdir))
  time.sleep(10*random.random())
  if random.random() < 0.5:
    log.error('ERROR! in workdir {}'.format(workdir))
    raise IOError
  log.info("done {}".format(workdir))
    
def setup():  
  pool = multiprocessing.Pool(4)
  def submit(func,args = (),kwargs = {}):
    return pool.apply_async(func,args,kwargs)
  return submit

def node_status(dag,node):
  submitted = dag.node[node].has_key('result')
  ready = dag.node[node]['result'].ready() if submitted else False
  successful = dag.node[node]['result'].successful() if ready else False
  log.debug("node {}: submitted: {}, ready: {}, successful: {}".format(node,submitted,ready,successful))
  return submitted and ready and successful

def node_ran_and_failed(dag,node):
  submitted = dag.node[node].has_key('result')
  ready = dag.node[node]['result'].ready() if submitted else False
  successful = dag.node[node]['result'].successful() if ready else False
  log.debug("node {}: submitted: {}, ready: {}, successful: {}".format(node,submitted,ready,successful))
  if submitted and ready and not successful:
    return True
  return False

def upstream_ok(dag,node):
  upstream = dag.predecessors(node)
  log.debug("upstream nodes are {}".format(dag.predecessors(node)))
  if not upstream:
    return True
  return all(node_status(dag,x) for x in upstream)

def upstream_failure(dag,node):
  upstream = dag.predecessors(node)
  if not upstream:
    return False
  return any(node_ran_and_failed(dag,x) for x in upstream)

import IPython

def nodes_left(dag):
  nodes_with_hope = [node for node in dag.nodes() if not upstream_failure(dag,node)]
  nodes_not_running = [dag.node[node] for node in nodes_with_hope if not dag.node[node].has_key('result')]
  if nodes_not_running:
    log.info('{} nodes that could be run are left.'.format(len(nodes_not_running)))
    return True
  else:
    log.info('no nodes can be run anymore')
    return False

def main():

  dag = random_dag(3,2)

  nx.write_dot(dag, 'dag.dot')
  
  for node in nx.topological_sort(dag):
    print '{} depends on {}'.format(node,dag.predecessors(node))
    
  submit = setup()

  with open('dag.png','w') as pngfile:
    subprocess.call(['dot','-Tpng','dag.dot'], stdout = pngfile)


  #while we have nodes that can be submitted
  while nodes_left(dag):
    for node in nx.topological_sort(dag):
      log.debug("working on node: {}".format(node))
      if dag.node[node].has_key('result'):
        log.debug("node {} already submitted. continue".format(node))
        continue;
      if upstream_ok(dag,node):
        log.info('submitting node: {}'.format(node))
        result = submit(hello,('workdir_node_{}'.format(node),))
        dag.node[node].update(result = result)
        
        
    time.sleep(1)
  
  #wait for all results to finish
  for node in dag.nodes():
    if dag.node[node].has_key('result'):
      dag.node[node]['result'].wait()

  print "done"

if __name__=='__main__':
  main()