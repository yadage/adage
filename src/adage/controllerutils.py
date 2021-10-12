import networkx as nx
import logging
import time
import datetime
import adage.dagstate as dagstate

log = logging.getLogger(__name__)

def connect_backend(adageobj,backend):
    for node in adageobj.dag.nodes():
        #check node status one last time so we pick up the finishing times
        adageobj.dag.getNode(node).backend = backend

def validate_finished_dag(dag):
    '''
    check for correct temporal execution order

    :param dag: graph object
    :return: ``True`` if order correct, ``False`` otherwise
    '''

    for node in dag:
        nodeobj = dag.getNode(node)
        if nodeobj.submit_time:
            for x in dag.predecessors(node):
                prednode = dag.getNode(x)
                if not nodeobj.submit_time > prednode.ready_by_time:
                    log.error('??? apparently {} was submitted before predesessor was ready: {}'.format(nodeobj, prednode))
                    log.error('node was submitted at: {} {}'.format(datetime.datetime.fromtimestamp(nodeobj.submit_time), nodeobj))
                    log.error('predecessor finished at: {} {}'.format(datetime.datetime.fromtimestamp(prednode.ready_by_time), prednode))
                    return False
    return True

def nodes_left_or_rule_applicable(adageobj):
    '''
    :param adageobj: the adage workflow object

    Main worklflow status function. It checks whether the overall workflow is finished by checking whether
    any eligible node (i.e. one without upstream failure) has not yet finished, or if any rules are still
    applicable.

    :param adageobj: the adage workflow object
    :return:
        - True any eligible node are waiting to be run, or still running or a rule is applicable
        - False in all other cases.
    '''

    dag,rules = adageobj.dag, adageobj.rules
    nodes_we_could_run = [node for node in dag.nodes() if not dagstate.upstream_failure(dag,dag.getNode(node))]
    nodes_running_or_defined = [x for x in nodes_we_could_run if dagstate.node_defined_or_running(dag.getNode(x))]

    if any(rule.applicable(adageobj) for rule in rules):
        return True

    log.debug('nodes we could run: %s',nodes_we_could_run)
    if nodes_running_or_defined:
        log.debug('%s nodes that could be run or are running are left.',len(nodes_running_or_defined))
        log.debug('nodes are: %s', [dag.nodes[n] for n in nodes_running_or_defined])
        return True

    if any(rule.applicable(adageobj) for rule in rules):
        return True


    log.info('no nodes can be run anymore and no rules are applicable')
    return False

def submittable_nodes(adageobj):
    '''
    main generator to iterate through nodes in the DAG and yield the ones that are submittable

    :param adageobj: the adage workflow object
    '''
    log.debug("process DAG")
    dag = adageobj.dag
    for node in nx.topological_sort(dag):
        nodeobj = dag.getNode(node)
        log.debug("working on node: %s with obj %s",node,nodeobj)
        if nodeobj.submit_time:
            log.debug("node %s already submitted. continue", nodeobj)
            continue
        if dagstate.upstream_ok(dag,nodeobj):
            log.debug("node submittable %s", nodeobj)
            yield nodeobj
        # if dagstate.upstream_failure(dag,nodeobj):
        #     log.debug('not yielding node: %s due to upstream failure',node)

def submit_nodes(nodeobjs,backend):
    '''
    :param nodeobj: the node object, whose task to submit
    :param backend: the task execution backend
    :return: None

    basic submission of a task associated to a given node object
    '''

    for nodeobj in nodeobjs:
        # log.info('submitting node %s', nodeobj)
        nodeobj.resultproxy = backend.submit(nodeobj.task)
        nodeobj.submit_time = time.time()

def applicable_rules(adageobj):
    '''
    Generator to iterate over applicable rules.
    yields the applicable rules (and their indices in the rules array)

    :param adageobj: the adage workflow object
    '''
    for rule in adageobj.rules:
        if rule.applicable(adageobj):
            yield rule
        else:
            log.debug('rule %s not ready yet',rule)

def apply_rules(adageobj, rules):
    '''
    :param adageobj: the adage workflow object
    :param rules: list of rule to apply
    :return: None

    applies a number of rules. The order of application is an implementation detail
    and the client must not assume on any specific of application
    '''

    for rule in rules:
        # log.info('applying rule %s', rule)
        rule.apply(adageobj)
        rule_index = adageobj.rules.index(rule)
        log.debug('popping rule at index %s', rule_index)
        adageobj.applied_rules.append(adageobj.rules.pop(rule_index))


def sync_state(adageobj,backend = None):
    '''
    Synchronize with Backend to update node processing status

    :param adageobj: the adage workflow object
    :return: None
    '''
    # log.info('sync state against backend')
    for node in adageobj.dag.nodes():
        #check node status one last time so we pick up the finishing times
        adageobj.dag.getNode(node).update_state(backend = backend)

def update_coroutine(adageobj):
    '''
    loops over applicable coroutines, applies them and manages the bookkeeping (moving rules from 'open' to 'applied')
    :param adageobj: the adage workflow object
    '''
    for i,rule in applicable_rules(adageobj):
        do_apply = yield rule
        if do_apply:
            log.debug('extending graph.')
            apply_rules(adageobj, [rule])
        yield
