import logging
import adage.nodestate as nodestate

log = logging.getLogger(__name__)

def node_ran_and_failed(nodeobj):
    '''
    :param nodeobj: the node object
    :return:

        - ``True`` if node has been processed and failed
        - ``False`` in all other cases
    '''
    return nodeobj.state == nodestate.FAILED

def upstream_ok(dag,nodeobj):
    upstream = dag.predecessors(nodeobj.identifier)
    log.debug("upstream nodes are %s",dag.predecessors(nodeobj.identifier))
    if not upstream:
        return True
    return all(node_status(dag.getNode(x)) for x in upstream)

def upstream_ready(dag,nodeobj):
    upstream = dag.predecessors(nodeobj.identifier)
    if not upstream:
        return True
    return all(dag.getNode(x).ready() for x in upstream)

def upstream_failure(dag,nodeobj):
    upstream = [dag.getNode(x) for x in dag.predecessors(nodeobj.identifier)]
    if not upstream:
        return False

    log.debug('checking upstream nodes %s',upstream)
    upstream_status = [node_ran_and_failed(obj) or upstream_failure(dag,obj) for obj in upstream]
    log.debug('upstream %s', 'ok' if upstream_status else 'failed')
    return any(upstream_status)

def node_status(nodeobj):
    '''
    boolean check on node status. 

    :param nodeobj: the node object
    :return:

        - ``True`` if successful (i.e. has beedn submitted, finished processing and exited successfully)
        - ``False`` in all other cases
    '''    
    submitted = nodeobj.submit_time
    ready = nodeobj.ready()
    successful = nodeobj.successful()
    log.debug("node %s: submitted: %s, ready: %s, successful: %s",nodeobj.identifier,submitted,ready,successful)

    return submitted and ready and successful
    
def node_defined_or_running(nodeobj):
    running = (nodeobj.state == nodestate.RUNNING)
    defined = (nodeobj.state == nodestate.DEFINED)
    log.debug('defined: %s running %s',defined,running)
    return running or defined
