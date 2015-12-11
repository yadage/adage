import logging
import time
import nodestate

log = logging.getLogger(__name__)

def node_ran_and_failed(nodeobj):
  return nodeobj.state() == nodestate.FAILED

def upstream_ok(dag,nodeobj):
  upstream = dag.predecessors(nodeobj.identifier)
  log.debug("upstream nodes are {}".format(dag.predecessors(nodeobj.identifier)))
  if not upstream:
    return True
  return all(node_status(dag.getNode(x)) for x in upstream)

def upstream_failure(dag,nodeobj):
  upstream = [dag.getNode(x) for x in dag.predecessors(nodeobj.identifier)]
  if not upstream:
    return False

  log.debug('checking upstream nodes {}'.format(upstream))
  upstream_status = [node_ran_and_failed(obj) or upstream_failure(dag,obj) for obj in upstream]
  log.debug('upstream failed: {}'.format(upstream_status))
  return any(upstream_status)

def node_status(nodeobj):
  state = nodeobj.state()
  submitted = nodeobj.submitted
  ready = nodeobj.ready()
  successful = nodeobj.successful()
  log.debug("node {}: submitted: {}, ready: {}, successful: {}".format(nodeobj.identifier,submitted,ready,successful))

  return submitted and ready and successful
  
def node_defined_or_waiting(nodeobj):
  running = (nodeobj.state() == nodestate.RUNNING)
  defined = (nodeobj.state() == nodestate.DEFINED)

  log.debug('defined: {} running {}'.format(defined,running))
  return running or defined
