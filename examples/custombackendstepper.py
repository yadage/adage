import custombackend
import adage

custombackend.create_state()
x = custombackend.WORKFLOWDATA.get()

backend  = custombackend.CustomBackend()
state = custombackend.load(x,backend)

def decide_rule(rule,state):
    print 'we could extend DAG with rule: {}. current state: {}'.format(rule,state)
    shall = raw_input("Shall we? (y/N) ").lower() == 'y'
    if shall:
        print 'ok we will extend.'
    else:
        print 'ok maybe another time'
    return shall

def decide_step(dag,nodeobj):
    print 'we could submit a DAG node (id: {}) DAG is: {}'.format(nodeobj,dag)
    shall = raw_input("Shall we? (y/N) ").lower() == 'y'
    if shall:
        print 'ok we will submit.'
    else:
        print 'do not submit for now'
    return shall

def advance_coroutine(coroutine):
    try:
        return coroutine.next()
    except AttributeError:
        return coroutine.__next__()

def custom_decider(decide_func):
    # we yield until we receive some data via send()
    def decider():
        data = yield
        while True:
            data = yield decide_func(*data)
    return decider

extend_decider = custom_decider(decide_rule)()
advance_coroutine(extend_decider) #prime decider

submit_decider = custom_decider(decide_step)()
advance_coroutine(submit_decider) #prime decider

coroutine = adage.adage_coroutine(backend,extend_decider,submit_decider)
advance_coroutine(coroutine)      #prime the coroutine....
coroutine.send(state)

try:
    state = advance_coroutine(coroutine)
    custombackend.save(state,custombackend.CustomJSONEncoder)
except StopIteration:
    custombackend.save(state,custombackend.CustomJSONEncoder)
    from adage.trackers import GifTracker
    import os
    g = GifTracker('custom.gif','{}/gifmaker'.format(os.getcwd()), frames = 10)
    g.finalize(state)
    print 'the worklow is done. exiting.'
    
