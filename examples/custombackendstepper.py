import custombackend
import adage

custombackend.create_state()
x = custombackend.WORKFLOWDATA.get()

backend  = custombackend.CustomBackend()
state = custombackend.load(x,backend)

def decide(rule,state):
    print 'we could extend DAG with rule: {}. current state: {}'.format(rule,state)
    shall = raw_input("Shall we? (y/N) ").lower() == 'y'
    if shall:
        print 'ok we will extend.'
    return shall

def custom_decider():
    # we yield until we receive some data via send()
    data = yield
    while True:
        data = yield decide(*data)

decider = custom_decider()
decider.next() #prime decider

coroutine = adage.adage_coroutine(backend,decider)
coroutine.next() #prime the coroutine....
coroutine.send(state)
try:
    state = coroutine.next()
    custombackend.save(state,custombackend.CustomJSONEncoder)
except StopIteration:
    custombackend.save(state,custombackend.CustomJSONEncoder)
    from adage.trackers import GifTracker
    import os
    g = GifTracker('custom.gif','{}/gifmaker'.format(os.getcwd()), frames = 10)
    g.finalize(state)
    print 'the worklow is done. exiting.'
    
