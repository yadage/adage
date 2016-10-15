import adage
import logging
import time
logging.basicConfig()

@adage.adagetask
def hello(one, two):
    print 'calling a task with ', one, two
    time.sleep(1)
    return 'a return value'


workflow = adage.adageobject()
initial = workflow.dag.addTask(hello.s(one = 'hello', two = 'there'))
another = workflow.dag.addTask(hello.s(one = 'one', two = 'two'))

@adage.decorators.callbackrule(after = {'init': initial.identifier, 'another': another.identifier})
def schedule(depnodes, adageobj):
    results = {k:v.result for k,v in depnodes.iteritems()}
    parts = results['init'].split()
    for i,p in enumerate(parts):
        adageobj.dag.addTask(hello.s(one = 'part {}'.format(i), two = p), nodename = p, depends_on = depnodes.values())

workflow.rules = [schedule]
adage.rundag(workflow, default_trackers = True, workdir = 'callback')
