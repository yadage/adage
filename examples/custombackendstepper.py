import custombackend
import adage

custombackend.create_state()
x = custombackend.WORKFLOWDATA.get()

backend  = custombackend.CustomBackend()
state = custombackend.load(x,backend)
ym = adage.yes_man()
ym.next() #prime decider

coroutine = adage.adage_coroutine(backend,ym)
coroutine.next() #prime the coroutine....
coroutine.send(state)
try:
    state = coroutine.next()
    custombackend.save(state,custombackend.CustomJSONEncoder)
    print 'STEPPED'
except StopIteration:
    custombackend.save(state,custombackend.CustomJSONEncoder)
    from adage.trackers import GifTracker
    import os
    g = GifTracker('custom.gif','{}/gifmaker'.format(os.getcwd()), frames = 10)
    g.finalize(state)
    print 'DONE'
    
