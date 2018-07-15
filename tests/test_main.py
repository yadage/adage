import adage
import logging
import time
import adage.serialize
import adage.node

logging.basicConfig(level = logging.INFO)

class rl(object):
    def when(self,func):
        self.predicate = func

    def do(self,func):
        self.body = func

    def applicable(self,adageobj):
        return self.predicate(adageobj)

    def apply(self,adageobj):
        return self.body(adageobj)

@adage.adagetask
def task(one):
    print('task! ',one)
    import time
    time.sleep(0.1)


def test_trivial():
    adageobj = adage.adageobject()
    adage.rundag(adageobj, default_trackers = False)

def test_simpleexample():
    x = rl()
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
    adageobj = adage.adageobject()
    adageobj.rules.append(x)
    adage.serialize.obj_to_json(adageobj, lambda r: None, lambda n: None)
    adage.rundag(adageobj, default_trackers = True)

    data = adage.serialize.obj_to_json(adageobj,lambda r: None, lambda n: adage.serialize.node_to_json(n,lambda t: {}, lambda p: {}))
    adage.serialize.dag_from_json(data['dag'], lambda n: adage.node.Node(n['name'],n['task'],n['id']))
