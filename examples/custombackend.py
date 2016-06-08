import adage.backends
from adage import adagetask, adageop, Rule, adageobject
import logging
import json
import uuid

log = logging.getLogger(__name__)

BACKENDDATA = None
WORKFLOWDATA = None

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
        log.info('setting rules')
        self.obj.rules = newrules

    @property
    def applied_rules(self):
        log.info('giving out applied rules')
        return self.obj.applied_rules

    @applied_rules.setter
    def applied_rules(self,newrules):
        log.info('setting applied rules')
        self.obj.applied_rules = newrules


import adage.serialize
class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, CustomState):
            return adage.serialize.obj_to_json(obj,
                ruleserializer = lambda r: r.json(),
                proxyserializer = lambda p: p.json() if p else None,
                taskserializer = lambda t: t)
        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, obj)

class CustomNode(adage.node.Node):
    def __init__(self,task,nodename,identifier = None):
        super(CustomNode,self).__init__(identifier = identifier, task = task, name = nodename)
    
    def __repr__(self):
        return '<Custom Node: {}>'.format(self.name)

class CustomRule(object):
    def __init__(self,predicate,body):
        self.pred = predicate
        self.body = body

    def json(self):
        return {'pred':self.pred,'body':self.body}

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
    depnode = None
    if 'depend' in taskspec:
        depnode = adageobj.dag.getNodeByName(taskspec['depend'])
    adageobj.dag.addNode(node , depends_on = [depnode] if depnode else [])

@adagetask
def atask(taskspec):
    print 'executing: atask...',taskspec
    import time
    time.sleep(10)

class CustomTracker(object):
    def initialize(self,adageobj):
        self.track(adageobj)
        log.info('initializing tracker')
        
    def track(self,adageobj):
        jsondata = json.dumps(adageobj, cls = CustomJSONEncoder)
        WORKFLOWDATA.commit(json.loads(jsondata))
        log.info('tracking using custom JSON tracker')
        
    def finalize(self,adageobj):
        self.track(adageobj)
        log.info('finalizing tracker')

def save(state,encoder):
    jsondata = json.dumps(state, cls = encoder)
    WORKFLOWDATA.commit(json.loads(jsondata))
    log.info('tracking using custom JSON tracker')

    
def load(jsondata,backend):
    statedict = {
        'DEFINED': adage.nodestate.DEFINED,
        'RUNNING': adage.nodestate.RUNNING,
        'FAILED':  adage.nodestate.FAILED,
        'SUCCESS': adage.nodestate.SUCCESS,
    }
    
    newstate = CustomState()
    for jsonnode in jsondata['dag']['nodes']:
        node = CustomNode(identifier = jsonnode['id'], task = jsonnode['task'], nodename = jsonnode['name'])
        node._state = statedict[jsonnode['state']]
        node.resultproxy = CustomProxy(jsonnode['proxy']['proxyid'],jsonnode['proxy']['task']) if jsonnode['proxy'] else None
        node.define_time = jsonnode['timestamps'].get('defined',None)
        node.submit_time = jsonnode['timestamps'].get('submit',None)
        node.ready_by_time = jsonnode['timestamps'].get('ready by',None)
        node.backend = backend
        newstate.dag.addNode(node)
   
    for fromnode,tonode in jsondata['dag']['edges']:
        newstate.dag.add_edge(fromnode,tonode)
        
    for jsonrule in jsondata['rules']:
        newstate.rules += [CustomRule(jsonrule['pred'],jsonrule['body'])]

    for jsonrule in jsondata['applied']:
        newstate.applied_rules += [CustomRule(jsonrule['pred'],jsonrule['body'])]
    
    return newstate
        
def main():
    logging.basicConfig(level = logging.DEBUG)
    create_state({'proxies':{},'results':{},'proxystate':{}},{})
    backend  = CustomBackend()
    adageobj = CustomState()
    adageobj.rules = []
    adageobj.rules += [
        CustomRule({'type':'always'}, {'type':'typeA', 'name': 'typeAnode'}),
    ]
    adageobj.rules += [
        CustomRule({'type':'byname', 'name':'typeAnode'}, {'type':'typeB', 'name': 'typeBnode','depend':'typeAnode'})
    ]
    save(adageobj,CustomJSONEncoder)

if __name__ == '__main__':
    main()