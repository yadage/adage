import networkx as nx
import logging
import os
import dagstate
import time
import trackers
import nodestate
from adage.decorators import adageop, adagetask, Rule
from adage.adageobject import adageobject

#silence pyflakes
assert adageop
assert adagetask
assert Rule
assert adageobject


log = logging.getLogger(__name__)

def validate_finished_dag(dag):
    '''
    check for correct temporal execution order

    :param dag: graph object
    :return: ``True`` if order correct, ``False`` otherwise
    '''

    for node in dag:
        nodeobj = dag.getNode(node)
        if nodeobj.submit_time:
            sanity = all([nodeobj.submit_time > dag.getNode(x).ready_by_time for x in dag.predecessors(node)])
            if not sanity:
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
        log.debug('nodes are: %s',[dag.node[n] for n in nodes_running_or_defined])
        return True

    if any(rule.applicable(adageobj) for rule in rules):
        return True


    log.info('no nodes can be run anymore and no rules are applicable')
    return False

def applicable_rules(adageobj):
    '''
    Generator to iterate over applicable rules.
    yields the applicable rules (and their indices in the rules array)

    :param adageobj: the adage workflow object
    '''
    for i,rule in reversed([x for x in enumerate(adageobj.rules)]):
        if rule.applicable(adageobj):
            yield i,rule
        else:
            log.debug('rule %s not ready yet',rule)

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
            log.debug("node already submitted. continue")
            continue;
        if dagstate.upstream_ok(dag,nodeobj):
            yield nodeobj
        # if dagstate.upstream_failure(dag,nodeobj):
        #     log.debug('not yielding node: %s due to upstream failure',node)

def apply_rules(adageobj, rules):
    '''
    :param adageobj: the adage workflow object
    :param rules: list of rule to apply
    :return: None

    applies a number of rules. The order of application is an implementation detail
    and the client must not assume on any specific of application
    '''
    for rule in rules:
        rule.apply(adageobj)

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
            adageobj.applied_rules.append(adageobj.rules.pop(i))
        yield

def update_dag(adageobj,decider):
    '''
    :param adageobj: the adage workflow object
    :param decider: a decision coroutine. 

    Higher level DAG update routine that calls the basic update loop recursively (
    in order to apply as many DAG extensions as possible). The `decider` coroutine will
    receive a (rule, adageobj) tuple and is expected to return control by yielding
    a boolean value
    '''
    anyapplied = False
    update_loop = update_coroutine(adageobj)
    for possible_rule in update_loop:
        log.debug('we could update this with rule: %s',possible_rule)
        command = decider.send((possible_rule,adageobj))
        if command:
            log.debug('we are in fact updating this..')
            anyapplied = True
        update_loop.send(command)
    #we changed the state so let's just recurse
    if anyapplied:
        log.debug('we applied a change, so we will recurse to see if we can apply anything else give updated state')
        update_dag(adageobj,decider)

def submit_node(nodeobj,backend):
    '''
    :param nodeobj: the node object, whose task to submit
    :param backend: the task execution backend
    :return: None

    basic submission of a task associated to a given node object
    '''

    nodeobj.resultproxy = backend.submit(nodeobj.task)
    nodeobj.submit_time = time.time()
    if not nodeobj.backend:
        nodeobj.backend = backend

def process_dag(backend,adageobj,decider):
    '''
    main loop to go through nodes in the DAG and submit the onces that are submittable
    '''
    log.debug("process DAG")
    dag = adageobj.dag
    for nodeobj in submittable_nodes(adageobj):
        do_submit = decider.send((dag,nodeobj))
        if do_submit:
            log.info('submitting %s job',nodeobj)
            submit_node(nodeobj,backend)

def update_state(adageobj):
    log.debug("update DAG")
    for node in adageobj.dag.nodes():
        #check node status one last time so we pick up the finishing times
        dagstate.node_status(adageobj.dag.getNode(node))

def adage_coroutine(backend,extend_decider,submit_decider):
    '''the main loop as a coroutine, for sequential stepping'''
    # after priming the coroutine, we yield right away until we get send a state object
    state = yield

    # after receiving the state object, we yield and will start the loop once we regain controls
    yield

    #starting the loop
    while nodes_left_or_rule_applicable(state):
        log.debug("main coroutine body start")
        update_dag(state,extend_decider)
        process_dag(backend,state,submit_decider)
        update_state(state)
        #we're done for this tick, let others proceed
        yield state

def yes_man(messagestring = 'received %s and %s'):
    '''trivial decision function that just returns True always'''
    # we yield until we receive some data via send()
    data = yield
    while True:
        rule, state = data
        log.debug(messagestring,rule,state)
        #we yield True and wait again to receive some data
        value = True
        data = yield value

def trackprogress(trackerlist,adageobj, method = 'track'):
    '''
    track adage workflow state using a list of trackers

    :param trackerlist: the stackers which should inspect the workflow
    :param adageobj: the adage workflow object
    :param method: tracking method to call. Must be one of ``initialize``, ``track``, ``finalize``
    :return: None
    '''
    map(lambda t: getattr(t,method)(adageobj), trackerlist)

def rundag(adageobj,
           backend = None,
           extend_decider = None,
           submit_decider = None,
           update_interval = 0.01,
           loggername = None,
           trackevery = 1,
           workdir = None,
           default_trackers = True,
           additional_trackers = None
           ):
    '''
    Main adage entrypoint. It's a convenience wrapper around the main adage coroutine loop and
    sets up the backend, logging, tracking (for GIFs, Text Snapshots, etc..) and possible interactive
    hooks into the coroutine

    :param adageobj: the adage workflow object
    :param backend: the task execution backend to which to submit node tasks
    :param extend_decider: decision coroutine to deal with whether to extend the workflow graph
    :param submit_decider: decision coroutine to deal with whether to submit node tasks
    :param update_interval: minimum looping interval for main adage loop 
    :param loggername: python logger to use
    :param trackevery: tracking interval for default simple report tracker
    :param workdir: workdir for default visual tracker
    :param default_trackers: whether to enable default trackers (simple report, gif visualization, text snapshot)
    :param additional_trackers: list of any additional tracking objects
    '''

    if loggername:
        global log
        log = logging.getLogger(loggername)

    ## funny behavior of multiprocessing Pools means that
    ## we can not have backendsubmit = multiprocsetup(2)    in the function sig
    ## so we only initialize them here
    if not backend:
        from backends import MultiProcBackend
        backend = MultiProcBackend(2)


    ## prepare the decision coroutines...
    if not extend_decider:
        extend_decider = yes_man('say yes to graph extension by rule: %s state: %s')
        extend_decider.next()

    if not submit_decider:
        submit_decider = yes_man('say yes to node submission of: %s%s')
        submit_decider.next()

    ## prepare tracking objects
    trackerlist = []
    if default_trackers:
        if not workdir:
            workdir = os.getcwd()
        trackerlist = [trackers.SimpleReportTracker(log,trackevery)]
        trackerlist += [trackers.GifTracker(gifname = '{}/workflow.gif'.format(workdir), workdir = '{}/track'.format(workdir))]
        trackerlist += [trackers.TextSnapShotTracker(logfilename = '{}/adagesnap.txt'.format(workdir), mindelta = trackevery)]
    if additional_trackers:
        trackerlist += additional_trackers


    log.info('preparing adage coroutine.')
    coroutine = adage_coroutine(backend,extend_decider,submit_decider)
    coroutine.next() #prime the coroutine....
    coroutine.send(adageobj)
    log.info('starting state loop.')

    try:
        trackprogress(trackerlist, adageobj, method = 'initialize')
        for state in coroutine:
            trackprogress(trackerlist,state)
            time.sleep(update_interval)
    except:
        log.exception('some weird exception caught in adage process loop')
        raise
    finally:
        trackprogress(trackerlist, adageobj, method = 'finalize')

    log.info('adage state loop done.')

    if adageobj.rules:
        log.warning('some rules were not applied.')

    log.info('validating execution')
    if not validate_finished_dag(adageobj.dag):
        log.error('DAG execution not validating')
        raise RuntimeError('DAG execution not validating')

    log.info('execution valid. (in terms of execution order)')
    if any(adageobj.dag.getNode(x).state == nodestate.FAILED for x in adageobj.dag.nodes()):
        log.error('raising RunTimeError due to failed jobs')
        raise RuntimeError('DAG execution failed')
