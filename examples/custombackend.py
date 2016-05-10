import adage.backends
from adage import adagetask, adageop ,Rule
import logging
import time
import os
import multiprocessing

logging.basicConfig(level = logging.DEBUG)
log = logging.getLogger(__name__)

backenddata = {'some':'backend'}
workflowdata = {'some':'workflowdata'}

import pickle


print '------'
pickle.dump(backenddata,open('bla.pickle','w'))
backenddata = pickle.load(open('bla.pickle'))
print backenddata
print '------'

@adagetask
def atask(taskspec):
    print 'executing: atask...',taskspec
    import time
    time.sleep(10)

class CustomProxy(object):        
    def __init__(self,task,proxy):
        self.task = task
        self.proxy = proxy
        log.info('created proxy...')
        
    def ready(self):
        return self.proxy.ready()
    
    def successful(self):
        return self.proxy.ready() and self.proxy.successful()

    def result(self):
        if self.successful():
            return self.proxy.get()
        return None
    
    
class CustomBackend(object):
    def __init__(self):
        self.pool = multiprocessing.Pool(1)

    def submit(self,task):
        log.info('submitting task %s',task)
        proxy = self.pool.apply_async(atask.s(taskspec = task))
        log.info('submitted task %s',task)
        return CustomProxy(task,proxy)

    def result(self,resultproxy):
        log.info('checking result')
        return resultproxy.result()
    
    def ready(self,resultproxy):
        log.info('checking ready')
        return resultproxy.ready()
    
    def successful(self,resultproxy):
        log.info('checking successful')
        return resultproxy.successful()
    
    def fail_info(self,resultproxy):
        return 'cannot give reason :( '

class CustomRule(object):
    def __init__(self,predicate,body):
        self.pred = predicate
        self.body = body

    def applicable(self,adageobj):
        return metapred(adageobj,self.pred)
        
    def apply(self,adageobj):
        return metabody(adageobj,self.body)

def metapred(adageobj,rulespec):
    log.info('checking predicate',rulespec)
    if rulespec['type']=='byname':
        value = adageobj.dag.getNodeByName(rulespec['name']) is not None
        return value
    elif rulespec['type']=='always':
        return True

def metabody(adageobj,taskspec):
    log.info('applying body',taskspec)
    adageobj.dag.addTask({'tasktype':taskspec['type']}, depends_on = [], nodename = taskspec['name'])

class CustomTracker(object):
    def initialize(self,adageobj):
        log.info('initializing tracker')
        
    def track(self,adageobj):
        log.info('tracking using tracker')
        
    def finalize(self,adageobj):
        log.info('finalizing tracker')

def main():

    backend = CustomBackend()

    adageobj = adage.adageobject()
    
    adageobj.rules = [
        CustomRule({'type':'always'}, {'type':'typeA', 'name': 'typeAnode'}),
        CustomRule({'type':'byname', 'name':'typeAnode'}, {'type':'typeB', 'name': 'typeBnode'})
    ]
    
    mytrack = CustomTracker()
    try:
        adage.rundag(adageobj, backend = backend, track = True, additional_trackers = [mytrack], workdir = 'simpleTrack', update_interval = 10, trackevery = 10)
    except RuntimeError:
        log.error('ERROR')

if __name__ == '__main__':
    main()