import adage.backends
from adage import adagetask, adageop, Rule, adageobject
import logging

logging.basicConfig(level = logging.DEBUG)
log = logging.getLogger(__name__)

BACKENDDATA = None
WORKFLOWDATA = None

import json
import uuid

class StateInterface(object):
    def __init__(self,statefile,initdata = None):
        self.statefile = statefile
        if initdata:
            log.info('creating state using initial data')
            self.commit(initdata)
        
    def get(self):
        return json.load(open(self.statefile))
        
    def commit(self,data):
        json.dump(data,open(self.statefile,'w'))

def create_state(initbackend = None,initwflow = None):
    global BACKENDDATA
    global WORKFLOWDATA
    BACKENDDATA  = StateInterface('backendstate.json',initbackend)
    WORKFLOWDATA = StateInterface('workflowstate.json',initwflow)
        
class CustomProxy(object):
    
    @classmethod
    def createNewProxy(cls,task):
        proxyid = str(uuid.uuid1())
        
        instance = cls(proxyid,task)
        x = BACKENDDATA.get()
        x['proxies'][instance.identifier] = [instance.json()]
        x['proxystate'][instance.identifier] = 'CREATED'
        BACKENDDATA.commit(x)
        return instance
        
    def json(self):
        jsondata = {'type':'CustomProxy','task':self.task,'proxyid':self.identifier}
        return jsondata
        
    def __init__(self,identifier,task):
        self.identifier = identifier
        self.task = task
        log.info('created proxy with id %s',self.identifier)
        
    def ready(self):
        x = BACKENDDATA.get()
        state = x['proxystate'][self.identifier]
        log.info('ready state in external state: %s, ready: %s',state, state in ['SUCCESS','FAILED'])
        return state in ['SUCCESS','FAILED']
    
    def successful(self):
        x = BACKENDDATA.get()
        state = x['proxystate'][self.identifier]
        log.info('ready state in external state: %s, sucess: %s',state, state in ['SUCCESS'])
        return state in ['SUCCESS']
        
    def result(self):
        if self.successful():
            x = BACKENDDATA.get()
            result = x['results'][self.identifier]
            log.info('result in external state: %s',result)
            return result
        return None

class CustomBackend(object):
    def __init__(self):
        pass

    def submit(self,task):
        log.info('submitting task %s',task)
        return CustomProxy.createNewProxy(task)

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

class CustomState(object):
    def __init__(self):
        self.obj = adage.adageobject()

    @property
    def dag(self):
        log.info('giving out DAG')
        return self.obj.dag
    
    @property
    def rules(self):
        log.info('giving out open rules')
        return self.obj.rules

    @rules.setter
    def rules(self,newrules):
        log.info('giving out open rules')
        self.obj.rules += newrules

    @property
    def applied_rules(self):
        log.info('giving out applied rules')
        return self.obj.applied_rules

class CustomNode(adage.node.Node):
    def __init__(self,task,nodename):
        super(CustomNode,self).__init__(task = task, name = nodename)
    
    def __repr__(self):
        return '<Custom Node: {}>'.format(self.name)

class CustomRule(object):
    def __init__(self,predicate,body):
        self.pred = predicate
        self.body = body

    def applicable(self,adageobj):
        return metapred(adageobj,self.pred)
        
    def apply(self,adageobj):
        return metabody(adageobj,self.body)

def metapred(adageobj,rulespec):
    log.info('checking predicate %s',rulespec)
    if rulespec['type']=='byname':
        node = adageobj.dag.getNodeByName(rulespec['name'])
        if not node:
            return False
        return node.successful()
    elif rulespec['type']=='always':
        return True

def metabody(adageobj,taskspec):
    log.info('applying body %s',taskspec)
    node = CustomNode(task = taskspec, nodename = taskspec['name'])
    adageobj.dag.addNode(node , depends_on = [])

@adagetask
def atask(taskspec):
    print 'executing: atask...',taskspec
    import time
    time.sleep(10)

class CustomTracker(object):
    def initialize(self,adageobj):
        log.info('initializing tracker')
        
    def track(self,adageobj):
        log.info('tracking using tracker')
        
    def finalize(self,adageobj):
        log.info('finalizing tracker')

def main():
    
    create_state({'proxies':{},'results':{},'proxystate':{}},{})
    backend  = CustomBackend()
    adageobj = CustomState()
    adageobj.rules = [
        CustomRule({'type':'always'}, {'type':'typeA', 'name': 'typeAnode'}),
        CustomRule({'type':'byname', 'name':'typeAnode'}, {'type':'typeB', 'name': 'typeBnode'})
    ]

    ym = adage.yes_man()
    ym.next() #prime decider

    coroutine = adage.adage_coroutine(backend,ym)
    coroutine.next() #prime the coroutine....


    coroutine.send(adageobj)

    # manual stepping...
    # for state in coroutine:
    #     print 'ready to step....'
    #     import IPython
    #     IPython.embed()

    # automated stepping...
    mytrack = CustomTracker()
    try:
        adage.rundag(adageobj,
                     backend = backend,
                     track = True,
                     additional_trackers = [mytrack],
                     workdir = 'simpleTrack',
                     update_interval = 30,
                     trackevery = 30)
    except RuntimeError:
        log.error('ERROR')
    # import IPython
    # IPython.embed()


if __name__ == '__main__':
    main()