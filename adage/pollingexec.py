import logging

log = logging.getLogger(__name__)

def update_coroutine(controller):
    '''
    loops over applicable coroutines, applies them and manages the bookkeeping (moving rules from 'open' to 'applied')
    :param controller: the adage workflow controller
    '''
    for rule in controller.applicable_rules():
        do_apply = yield rule
        if do_apply:
            log.debug('extending graph.')
            controller.apply_rules([rule])
        yield

def update_dag(controller, decider):
    '''
    :param controller: the adage workflow controller
    :param decider: a decision coroutine. 

    Higher level DAG update routine that calls the basic update loop recursively (
    in order to apply as many DAG extensions as possible). The `decider` coroutine will
    receive a (rule, controller) tuple and is expected to return control by yielding
    a boolean value
    '''
    log.debug("update DAG by submitting nodes")
    anyapplied = False
    update_loop = update_coroutine(controller)
    for possible_rule in update_loop:
        log.debug('we could update this with rule: %s',possible_rule)
        command = decider.send((possible_rule,controller))
        if command:
            log.debug('we are in fact updating this..')
            anyapplied = True
        update_loop.send(command)
    #we changed the state so let's just recurse
    if anyapplied:
        log.debug('we applied a change, so we will recurse to see if we can apply anything else give updated state')
        update_dag(controller, decider)


def process_dag(controller,decider):
    '''
    main loop to go through nodes in the DAG and submit the onces that are submittable
    '''
    log.debug("process DAG by submitting nodes")
    for nodeobj in controller.submittable_nodes():
        do_submit = decider.send((nodeobj,controller))
        if do_submit:
            controller.submit_nodes([nodeobj])

def adage_coroutine(extend_decider,submit_decider):
    '''
    :param extend_decider: decision coroutine to decide whether to apply applicable rules
    :param submit_decider: decision coroutine to decide whether to submit applicable nodes

    the main loop as a coroutine, for sequential stepping (repeated polling) through the workflow.
    the loop will go through a update->submit->sync cycle while yielding control to the decision
    coroutines
    '''

    # after priming the coroutine, we yield right away until we get send a state object

    controller = yield

    # after receiving the state object, we yield and will start the loop once we regain controls
    yield

    #starting the loop
    while not controller.finished():
        update_dag(controller, extend_decider)
        process_dag(controller,submit_decider)
        # we're done for this tick, let others proceed
        yield controller
    log.info('exiting main polling coroutine')

def yes_man(messagestring = 'saying yes.'):
    '''trivial decision function that just returns True always'''
    # we yield until we receive some data via send()
    data = yield
    while True:
        log.debug(messagestring,data)
        #we yield True and wait again to receive some data
        value = True
        data = yield value

def setup_polling_execution(extend_decider = None, submit_decider = None):
    '''
    sets up the main couroutine and auxiliary decision coroutines for polling-style
    workflow exectuion. Optionally decision coroutines can be passed as parameters 
    (must already be primed)

    :param extend_decider: decision coroutine to decide whether to apply applicable rules
    :param submit_decider: decision coroutine to decide whether to submit applicable nodes
    '''

    if not extend_decider:
        extend_decider = yes_man('say yes to graph extension by %s')
        extend_decider.next() # prime

    if not submit_decider:
        submit_decider = yes_man('say yes to node submission of: %s')
        submit_decider.next() # prime

    ## prep main coroutine with deciders..
    log.info('preparing adage coroutine.')
    coroutine = adage_coroutine(extend_decider,submit_decider)
    coroutine.next() # prime the coroutine....

    return coroutine
