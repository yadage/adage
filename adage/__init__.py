import multiprocessing
import time
import networkx as nx
import subprocess
import glob
import logging
import os
import shutil

logging.basicConfig(format='%(asctime)s %(message)s',level=logging.INFO)

log = logging.getLogger(__name__)
tasks = {}
validrules = {}

def print_next_dag(dag):
  nextnr = 0
  if glob.glob('track/*.dot'):
    nextnr = max([int(os.path.basename(s).replace('dag','').replace('.dot','')) for s in  glob.glob('track/*.dot')])+1
  
  padded = '{}'.format(nextnr).zfill(3)
  print_dag(dag,'dag{}'.format(padded))
  

def colorize_graph(dag):
  colorized = nx.DiGraph()
  for node in nx.topological_sort(dag):

    nodedict = dag.node[node].copy()
    nodedict.pop('result',None)

    if 'result' not in nodedict:
      nodedict['result'] = 'grey'
    if 'submitted' in nodedict and ('ready_by' not in nodedict):
      nodedict['result'] = 'yellow'
    if upstream_failure(dag,node):
      nodedict['result'] = 'blue'
    if node_ran_and_failed(dag,node):
      nodedict['result'] = 'red'
    if node_status(dag,node):
      nodedict['result'] = 'green'
    
    colorized.add_node(node,nodedict)
    for pre in dag.predecessors(node):
      colorized.add_edge(pre,node)

  for node in colorized.nodes():
    nodedict = colorized.node[node]
    label = '{}: {}/{}/{}'.format(nodedict['nodenr'],nodedict['taskname'],nodedict['args'],nodedict['kwargs'])
    nodedict.update(style='filled',color = nodedict['result'],label = label)
    
  return colorized

def print_dag(dag,name):
  dotfilename = 'track/{}.dot'.format(name)
  pngfilename = 'track/{}.png'.format(name) 
  colorized = colorize_graph(dag)

  nx.write_dot(colorized,dotfilename)
  with open(pngfilename,'w') as pngfile:
    subprocess.call(['dot','-Tpng','-Gsize=18,12\!','-Gdpi=100 ',dotfilename], stdout = pngfile)
    subprocess.call(['convert',pngfilename,'-gravity','North','-background','white','-extent','1800x1200',pngfilename])


def mk_dag():
  return nx.DiGraph()

def mknode(dag,sig, nodename = 'node', depends_on = []):
  assert sig
  nodenr = len(dag.nodes())
  dag.add_node(nodenr,
    nodenr = nodenr,
    nodename = nodename,
    taskname = sig[0],
    args = sig[1]['args'],
    kwargs = sig[1]['kwargs']
  )
  nodedict = dag.node[nodenr]
  for parent in depends_on:
    add_edge(dag,parent,nodedict)
  return nodedict
  




#similar idea as in celery
def signature(name,*args,**kwargs):
  return [name,{'args':args,'kwargs':kwargs}]

def qualifiedname(thing):
  if thing.__module__ != '__main__':
    return '{}.{}'.format(thing.__module__,thing.__name__)
  else:
    return thing.__name__

def adagetask(func):
  func.taskname = qualifiedname(func)
  def sig(*args,**kwargs):
    return signature(func.taskname,*args,**kwargs)
  func.s = sig
  tasks[func.taskname] = func
  return func

def rulefunc(func):
  func.rulename = qualifiedname(func)
  validrules[func.rulename] = func
  def sig(*args,**kwargs):
    return signature(func.rulename,*args,**kwargs)
  func.s = sig
  return func

def validate_finished_dag(dag):
  for node in dag:
    nodedict = dag.node[node]
    if 'submitted' in nodedict:
      sanity = all([nodedict['submitted'] > dag.node[x]['ready_by'] for x in dag.predecessors(node)])
      if not sanity:
        return False
  return True
    
def setup(poolsize):  
  pool = multiprocessing.Pool(poolsize)
  def submit(func,args = (),kwargs = {}):
    return pool.apply_async(func,args,kwargs)
  return submit

def node_status(dag,node):
  nodedict = dag.node[node]
  submitted = 'result' in nodedict
  ready = nodedict['result'].ready() if submitted else False
  successful = nodedict['result'].successful() if ready else False
  log.debug("node {}: submitted: {}, ready: {}, successful: {}".format(node,submitted,ready,successful))

  if ready and ('ready_by' not in nodedict):
    nodedict.update(ready_by = time.time())

  return submitted and ready and successful

def result_of(nodedict):
  return nodedict['result'].get()

def node_ran_and_failed(dag,node):
  nodedict = dag.node[node]
  submitted = 'result' in nodedict
  ready = nodedict['result'].ready() if submitted else False
  successful = nodedict['result'].successful() if ready else False
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

def get_node_by_name(dag,nodename):
  matching = [dag.node[x] for x in dag.nodes() if dag.node[x]['nodename'] == nodename]
  return matching[0] if (len(matching) == 1) else None
  
def add_edge(dag,from_node,to_node):
  dag.add_edge(from_node['nodenr'],to_node['nodenr'])
  

def node_running_or_waiting(dag,node):
  nodedict = dag.node[node]
  log.debug(nodedict)
  running = 'result' in nodedict and not nodedict['result'].ready()
  waiting = 'result' not in nodedict

  log.debug('waiting: {} running {}'.format(waiting,running))
  return running or waiting

def nodes_left_or_rule(dag,rules):
  nodes_we_could_run = [node for node in dag.nodes() if not upstream_failure(dag,node)]
  nodes_running_or_waiting = [x for x in nodes_we_could_run if node_running_or_waiting(dag,x)]

  if any(rule_applicable(dag,rule) for rule in rules):
    return True

  if nodes_running_or_waiting:
    log.info('{} nodes that could be run or are running are left.'.format(len(nodes_running_or_waiting)))
    return True
  else:
    log.info('no nodes can be run anymore')
    return False

def apply_rule(dag,ruletoapply):
  body_name,body_details = ruletoapply[1]
  extended_kwargs = body_details['kwargs'].copy()
  extended_kwargs.update(dag = dag)
  return validrules[body_name](*body_details['args'],**extended_kwargs)

def rule_applicable(dag,ruletoapply):
  predicate_name,predicate_details = ruletoapply[0]  
  extended_kwargs = predicate_details['kwargs'].copy()
  extended_kwargs.update(dag = dag)
  log.debug('running predicate {} with args {} and kwargs {}'.format(predicate_name,predicate_details['args'],extended_kwargs))
  return validrules[predicate_name](*predicate_details['args'],**extended_kwargs)

def rundag(dag,rules, track = False, poolsize = 2):
  log.info('running DAG with {} workers'.format(poolsize))
  submit = setup(poolsize)

  if track:
    if os.path.exists('./track'):
      shutil.rmtree('./track')
    os.makedirs('./track')
    print_next_dag(dag)

  for node in nx.topological_sort(dag):
    log.info('{} depends on {}'.format(node,dag.predecessors(node)))
    

  #while we have nodes that can be submitted
  while nodes_left_or_rule(dag,rules):
    #iterate rules in reverse so we can safely pop items
    for i,rule in reversed([x for x in enumerate(rules)]):
      if rule_applicable(dag,rule):
        log.info('applying a rule!')
        apply_rule(dag,rule)
        rules.pop(i)
        if track: print_next_dag(dag)
      else:
        log.info('rule not ready yet')
    
    for node in nx.topological_sort(dag):
      nodedict = dag.node[node]
      log.debug("working on node: {} with dict {}".format(node,nodedict))
      if 'result' in nodedict:
        log.debug("node {} already submitted. continue".format(node))
        continue;
      if upstream_ok(dag,node):
        log.info('submitting node: {}'.format(node))
        result = submit(tasks[nodedict['taskname']],nodedict['args'],nodedict['kwargs'])
        nodedict.update(result = result,submitted = time.time())

      if upstream_failure(dag,node):
        log.warning('not submitting node: {} due to upstream failure'.format(node))
              
    time.sleep(1)
    if track: print_next_dag(dag)
    

  #wait for all results to finish
  log.info('all running jobs are finished. waiting for running jobs..')
  for node in dag.nodes():
    if 'result' in dag.node[node]:
      dag.node[node]['result'].wait()
  log.info('all done.')

  for node in dag.nodes():
    #check node status one last time so we pick up the finishing times
    node_status(dag,node)

  if not validate_finished_dag(dag):
    log.error('DAG execution not validating')
    raise RuntimeError
  log.info('execution valid.')

  if track:
    print_next_dag(dag)
    subprocess.call('convert -delay 50 $(ls track/*.png|sort) workflow.gif',shell = True)
    shutil.rmtree('./track')

