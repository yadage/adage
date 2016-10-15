import adage
import logging

@adage.adagetask
def task(one):
    print 'task! ',one
    import time
    time.sleep(0.1)

class rl(object):
    def when(self,func):
        self.predicate = func
    
    def do(self,func):
        self.body = func
    
    def applicable(self,adageobj):
        return self.predicate(adageobj)
    
    def apply(self,adageobj):
        return self.body(adageobj)


adageobj = adage.adageobject()    
    
x = rl()
adageobj.rules.append(x)

@x.when
def _(adageobj):
    return True
        
@x.do
def _(adageobj):
    dep = adageobj.dag.addTask(task.s(one = 'what'), nodename = 'first')
    newrule = rl()
    adageobj.rules.append(newrule)
    @newrule.when
    def _(adageobj):
        upstream =  adageobj.dag.getNodeByName('first')
        return upstream.state == adage.nodestate.SUCCESS

    @newrule.do
    def _(adageobj):
        deptwo = adageobj.dag.addTask(task.s(one = 'ok...'), depends_on = [dep], nodename = 'what')
        newrule = rl()
        adageobj.rules.append(newrule)
        @newrule.when
        def _(adageobj):
            return True

        @newrule.do
        def _(adageobj):
            adageobj.dag.addTask(task.s(one = 'nested new'), nodename = 'the')
        

logging.basicConfig(level = logging.INFO)
adage.rundag(adageobj, default_trackers = True)

    
