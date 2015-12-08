import multiprocessing
import time
import networkx as nx
import subprocess
import glob
import logging
import os
import shutil
import sys
import visualize as viz
import dagstate

from decorators import adagetask,functorize,Rule, qualifiedname
from dagutils import *

log = logging.getLogger(__name__)

def validate_finished_dag(dag):
  for node in dag:
    nodeobj = get_nodeobj(dag,node)
    if nodeobj.submitted:
      sanity = all([nodeobj.submitted > get_nodeobj(dag,x).ready_by_time for x in dag.predecessors(node)])
      if not sanity:
        return False
  return True
    
def get_failure_info(backend,nodeobj):
  try:
    backend.result_of(nodeobj.result)
  except:
    log.info("node {} failed with error: {}".format(nodeobj,sys.exc_info()))

def get_node_by_name(dag,nodename):
  matching = [x for x in dag.nodes() if dag.node[x]['nodeobj'].name == nodename]
  return get_nodeobj(dag,matching[0]) if (len(matching) == 1) else None
  
  
def nodes_left_or_rule(dag,rules):
  nodes_we_could_run = [node for node in dag.nodes() if not dagstate.upstream_failure(dag,get_nodeobj(dag,node))]
  nodes_running_or_defined = [x for x in nodes_we_could_run if dagstate.node_defined_or_waiting(get_nodeobj(dag,x))]

  if any(rule.applicable(dag) for rule in rules):
    return True

  log.debug('nodes we could run: {}'.format(nodes_we_could_run))
  if nodes_running_or_defined:
    log.debug('{} nodes that could be run or are running are left.'.format(len(nodes_running_or_defined)))
    log.debug('nodes are: {}'.format([dag.node[n] for n in nodes_running_or_defined]))
    return True
  else:
    log.info('no nodes can be run anymore')
    return False

def apply_rule(dag,ruletoapply):
  return ruletoapply[1](dag)

def rule_applicable(dag,ruletoapply):
  return ruletoapply[0](dag)


def rundag(dag,rules, track = False, backend = None, loggername = None, workdir = None, trackevery = 1):
  if loggername:
    global log
    log = logging.getLogger(loggername)
  
  ## funny behavior of multiprocessing Pools means that
  ## we can not have backendsubmit = multiprocsetup(2)  in the function sig
  ## so we only initialize them here
  if not backend:
    from backends import MultiProcBackend
    backend = MultiProcBackend(2)

  if not workdir:
    workdir = os.getcwd()

  trackdir = '{}/track'.format(workdir)

  if track:
    if os.path.exists(trackdir):
      shutil.rmtree(trackdir)
    os.makedirs(trackdir)
    viz.print_next_dag(dag,trackdir)

  #while we have nodes that can be submitted
  trackcounter = 0
  while nodes_left_or_rule(dag,rules):
    
    #iterate rules in reverse so we can safely pop items
    for i,rule in reversed([x for x in enumerate(rules)]):
      if rule.applicable(dag):
        log.info('extending graph.')
        rule.apply(dag)
        rules.pop(i)
        if track: viz.print_next_dag(dag,trackdir)
      else:
        log.debug('rule not ready yet')
    
    for node in nx.topological_sort(dag):
      nodeobj = get_nodeobj(dag,node)

      if not nodeobj.backend:
        nodeobj.backend = backend

      log.debug("working on node: {} with obj {}".format(node,nodeobj))

      if nodeobj.submitted:
        log.debug("node already submitted. continue")
        continue;
      if dagstate.upstream_ok(dag,nodeobj):
        log.info('submitting {} job'.format(nodeobj))
        nodeobj.result = backend.submit(nodeobj.signature)
        submit_time = time.time()
        nodeobj.submitted = submit_time

      if dagstate.upstream_failure(dag,nodeobj):
        log.warning('not submitting node: {} due to upstream failure'.format(node))

    time.sleep(1)
    trackcounter+=1
    if track and trackcounter == trackevery:
      trackcounter=0
      viz.print_next_dag(dag,trackdir)
    
  log.info('all running jobs are finished.')

  for node in dag.nodes():
    #check node status one last time so we pick up the finishing times
    dagstate.node_status(get_nodeobj(dag,node))

  if not validate_finished_dag(dag):
    log.error('DAG execution not validating')
    raise RuntimeError
  log.info('execution valid. (in terms of execution order)')

  #collect some stats:
  successful = 0
  failed = 0
  notrun = 0
  for node in nx.topological_sort(dag):
    nodeobj = get_nodeobj(dag,node)
    if dagstate.node_status(nodeobj):
      successful+=1
    if dagstate.node_ran_and_failed(nodeobj):
      failed+=1
      log.info("node: {} failed. reason: {}".format(nodeobj,backend.fail_info(nodeobj.result)))
    if dagstate.upstream_failure(dag,nodeobj):
      notrun+=1

  log.info('successful: {} | failed: {} | notrun: {} | total: {}'
           .format(successful,failed,notrun,len(dag.nodes())))

  if track:
    log.info('producing visualization...')
    viz.print_next_dag(dag,trackdir)
    subprocess.call('convert -delay 50 $(ls {}/*.png|sort) {}/workflow.gif'.format(trackdir,workdir),shell = True)
    shutil.rmtree(trackdir)

  if failed:
    log.error('raising RunTimeError due to failed jobs')
    raise RuntimeError 
