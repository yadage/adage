import multiprocessing
import time
import random
import networkx as nx
import subprocess
import uuid
import logging
FORMAT = '%(asctime)s %(message)s'
logging.basicConfig(format=FORMAT,level=logging.INFO)

log = logging.getLogger(__name__)

def random_dag(nodes, edges):
    """Generate a random Directed Acyclic Graph (DAG) with a given number of nodes and edges."""
    G = nx.DiGraph()
    for i in range(nodes):
        G.add_node(i)
        G.node[i].update(
          nodeid = str(uuid.uuid1()),
          taskname = 'hello',
          args = ('workdir_node_{}'.format(i),),
          kwargs = {}
        )
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

def hello(workdir,*args,**kwargs):
  log.info("running job in workdir {}".format(workdir))
  time.sleep(10*random.random())
  
  log.info('kwargs: {}'.format(kwargs))

  if random.random() < 0:
    log.error('ERROR! in workdir {}'.format(workdir))
    raise IOError
  log.info("done {}".format(workdir))

tasks = {}
tasks['hello'] = hello

def node_present(nodeid,dag):
  parent = filter(lambda n:dag.node[n]['nodeid'] == nodeid,dag.nodes())
  if not parent:
    return False
  return True

def schedule_after_this(nodeid,taskinfo,dag):
  parent = filter(lambda n:dag.node[n]['nodeid'] == nodeid,dag.nodes())
  assert len(parent) == 1
  
  parentnr = parent[0]
  parent = dag.node[parentnr]

  nodenr = len(dag.nodes())
  dag.add_node(nodenr)
  dag.node[nodenr].update(
    nodeid = str(uuid.uuid1()),
    taskname = 'hello',
    args = ('dynamic_node_{}'.format(nodenr),),
    kwargs = {}
  )
  dag.add_edge(parentnr,nodenr)
    
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

  if (submitted and ready):
    if not successful:
      log.debug('node {}: ran {}, failed {}'.format(node,submitted and ready,successful) )
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

  upstream_status = [node_ran_and_failed(dag,x) or upstream_failure(dag,x) for x in upstream]
  log.debug('upstream status: {}'.format(upstream_status))
  return any(upstream_status)

import IPython

def nodes_left(dag):
  nodes_we_could_run = [node for node in dag.nodes() if not upstream_failure(dag,node)]
  nodes_not_running = [dag.node[node] for node in nodes_we_could_run if not dag.node[node].has_key('result')]
  if nodes_not_running:
    log.info('{} nodes that could be run are left.'.format(len(nodes_not_running)))
    return True
  else:
    log.info('no nodes can be run anymore')
    return False

def apply_rule(dag,ruleset,ruletoapply):
  rulename = ruletoapply[0]
  ruleinfo = ruletoapply[1]

  predkwargs = ruleinfo['predicate']['kwargs'].copy()
  predkwargs.update(dag = dag)

  if(ruleset[rulename]['predicate'](*ruleinfo['predicate']['args'],**predkwargs)):
    bodykwargs = ruleinfo['body']['kwargs'].copy()
    bodykwargs.update(dag = dag)
    ruleset[rulename]['body'](*ruleinfo['body']['args'],**bodykwargs)

def main():

  dag = random_dag(3,2)


  ruleset = {}
  ruleset['schedule_after_this'] = {'predicate':node_present,'body':schedule_after_this}


  rules = []
  rules += [['schedule_after_this',{'predicate':{'args':[dag.node[1]['nodeid'],],'kwargs':{} },
                                   'body': {'args':[dag.node[1]['nodeid'],None],'kwargs':{} }
                                   }
           ]]


  nx.write_dot(dag, 'dag1.dot')
  with open('dag1.png','w') as pngfile:
    subprocess.call(['dot','-Tpng','dag1.dot'], stdout = pngfile)

  
  for node in nx.topological_sort(dag):
    print '{} depends on {}'.format(node,dag.predecessors(node))
    
  submit = setup()



  #while we have nodes that can be submitted
  while nodes_left(dag):
    for node in nx.topological_sort(dag):
      log.debug("working on node: {}".format(node))
      if dag.node[node].has_key('result'):
        log.debug("node {} already submitted. continue".format(node))
        continue;
      if upstream_ok(dag,node):
        log.info('submitting node: {}'.format(node))
        nodedict = dag.node[node]


        nodedict['kwargs'].update(nodeid = nodedict['nodeid'])

        log.info(nodedict)
        
        result = submit(tasks[nodedict['taskname']],nodedict['args'],nodedict['kwargs'])
        dag.node[node].update(result = result)
      if upstream_failure(dag,node):
        log.warning('not submitting node: {} due to upstream failure'.format(node))
    time.sleep(1)
    if rules:
      log.info('applying a rule!')
      apply_rule(dag,ruleset,rules[0])
      rules.pop(0)
    
  
  #wait for all results to finish
  for node in dag.nodes():
    if dag.node[node].has_key('result'):
      dag.node[node]['result'].wait()

  log.info('all running jobs are finished.')

  nx.write_dot(dag, 'dag2.dot')
  with open('dag2.png','w') as pngfile:
    subprocess.call(['dot','-Tpng','dag2.dot'], stdout = pngfile)


if __name__=='__main__':
  main()