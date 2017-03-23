import logging

log = logging.getLogger(__name__)

def update_coroutine(controller):
    '''
    loops over applicable coroutines, applies them and manages the bookkeeping (moving rules from 'open' to 'applied')
    :param controller: the adage workflow controller
    '''
    for i,rule in controller.applicable_rules():
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
    log.debug("process DAG")
    for nodeobj in controller.submittable_nodes():
        do_submit = decider.send(controller)
        if do_submit:
            log.info('submitting %s job',nodeobj)
            controller.submit_nodes([nodeobj])

def adage_coroutine(extend_decider,submit_decider):
    '''the main loop as a coroutine, for sequential stepping'''
    # after priming the coroutine, we yield right away until we get send a state object
    controller = yield

    # after receiving the state object, we yield and will start the loop once we regain controls
    yield

    #starting the loop
    while not controller.finished():
        log.debug("main coroutine body start")
        update_dag(controller, extend_decider)
        process_dag(controller,submit_decider)
        controller.sync_backend()
        #we're done for this tick, let others proceed
        yield controller