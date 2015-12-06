import multiprocessing
import time
import networkx as nx
import subprocess
import glob
import logging
import os
import shutil
import sys

log = logging.getLogger(__name__)
tasks = {}
validrules = {}

def print_next_dag(dag,trackdir):
  nextnr = 0
  if glob.glob('{}/*.dot'.format(trackdir)):
    nextnr = max([int(os.path.basename(s).replace('dag','').replace('.dot','')) for s in  glob.glob('{}/*.dot'.format(trackdir))])+1
  
  padded = '{}'.format(nextnr).zfill(3)
  print_dag(dag,'dag{}'.format(padded),trackdir)
  

def colorize_graph(dag):
  colorized = nx.DiGraph()
  for node in dag.nodes():

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
    #label = '{}: {}/{}/{}'.format(nodedict['nodenr'],nodedict['taskname'],nodedict['args'],nodedict['kwargs'])
    label = nodedict['taskname']
    nodedict.update(style='filled',color = nodedict['result'],label = label)
    
  return colorized

def print_dag(dag,name,trackdir):
  dotfilename = '{}/{}.dot'.format(trackdir,name)
  pngfilename = '{}/{}.png'.format(trackdir,name) 
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
  
# similar idea as in celery
def signature(adagetaskname,*args,**kwargs):
  return [adagetaskname,{'args':args,'kwargs':kwargs}]

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
  try:
    from celery import shared_task
    func.celery = shared_task(func)
  except ImportError:
    pass
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
    
def multiprocsetup(poolsize):  
  pool = multiprocessing.Pool(poolsize)
  def submit(func,args = (),kwargs = {}):
    return pool.apply_async(func,args,kwargs)
  return submit

def celerysetup(app):
  app.set_current()
  def submit(func,args = (),kwargs = {}):
    return func.celery.apply_async(args,kwargs,throw = False)
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

def get_failure_info(nodedict):
  try:
    result_of(nodedict)
  except:
    log.info("node {} failed with error: {}".format(nodedict,sys.exc_info()))

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
  log.debug('upstream failed: {}'.format(upstream_status))
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
    log.debug('{} nodes that could be run or are running are left.'.format(len(nodes_running_or_waiting)))
    log.debug('nodes are: {}'.format([dag.node[n] for n in nodes_running_or_waiting]))
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

def rundag(dag,rules, track = False, backendsubmit = None, loggername = None, workdir = None, trackevery = 1):
  if loggername:
    global log
    log = logging.getLogger(loggername)
  
  ## funny behavior of multiprocessing Pools means that
  ## we can not have backendsubmit = multiprocsetup(2)  in the function sig
  ## so we only initialize them here
  if not backendsubmit:
    backendsubmit = multiprocsetup(2)

  if not workdir:
    workdir = os.getcwd()

  trackdir = '{}/track'.format(workdir)

  if track:
    if os.path.exists(trackdir):
      shutil.rmtree(trackdir)
    os.makedirs(trackdir)
    print_next_dag(dag,trackdir)

  #while we have nodes that can be submitted
  trackcounter = 0
  while nodes_left_or_rule(dag,rules):
    
    #iterate rules in reverse so we can safely pop items
    for i,rule in reversed([x for x in enumerate(rules)]):
      if rule_applicable(dag,rule):
        log.info('extending graph.')
        apply_rule(dag,rule)
        rules.pop(i)
        if track: print_next_dag(dag,trackdir)
      else:
        log.debug('rule not ready yet')
    
    for node in nx.topological_sort(dag):
      nodedict = dag.node[node]
      log.debug("working on node: {} with dict {}".format(node,nodedict))
      if 'result' in nodedict:
        log.debug("node already submitted. continue")
        continue;
      if upstream_ok(dag,node):
        log.info('submitting {} job'.format(nodedict ['taskname']))
        result = backendsubmit(tasks[nodedict['taskname']],nodedict['args'],nodedict['kwargs'])
        nodedict.update(result = result,submitted = time.time())

      if upstream_failure(dag,node):
        log.warning('not submitting node: {} due to upstream failure'.format(node))

    time.sleep(1)
    trackcounter+=1
    if track and trackcounter == trackevery:
      trackcounter=0
      print_next_dag(dag,trackdir)
    
  log.info('all running jobs are finished.')

  for node in dag.nodes():
    #check node status one last time so we pick up the finishing times
    node_status(dag,node)

  if not validate_finished_dag(dag):
    log.error('DAG execution not validating')
    raise RuntimeError
  log.info('execution valid. (in terms of execution order)')

  #collect some stats:
  successful = 0
  failed = 0
  notrun = 0
  for node in nx.topological_sort(dag):
    if node_status(dag,node):
      successful+=1
    if node_ran_and_failed(dag,node):
      failed+=1
      get_failure_info(dag.node[node])
    if upstream_failure(dag,node):
      notrun+=1

  log.info('successful: {} | failed: {} | notrun: {} | total: {}'
           .format(successful,failed,notrun,len(dag.nodes())))

  if track:
    log.info('producing visualization...')
    print_next_dag(dag,trackdir)
    subprocess.call('convert -delay 50 $(ls {}/*.png|sort) {}/workflow.gif'.format(trackdir,workdir),shell = True)
    shutil.rmtree(trackdir)

  if failed:
    log.error('raising RunTimeError due to failed jobs')
    raise RuntimeError 
