import adage
import adage.dagstate
from adage import adageop, Rule

import logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


from adage import adagetask

@adagetask
def boolean_task():
    import random
    val = (random.random() < 0.5)
    print 'i am returning True or False: {}'.format(val)
    return val

@adagetask
def first_option():
    print 'i am the first option'

@adagetask
def second_option():
    print 'i am the second option'

@adageop
def node_done(adageobj,nodename):
    node = adageobj.dag.getNodeByName(nodename)
    if node:
        return adage.dagstate.node_status(node)
    return False

@adageop
def schedule_if_else(adageobj,depnode):

    depnode = adageobj.dag.getNodeByName(depnode)
    result = depnode.result

    if result:
        adageobj.dag.addTask(first_option.s(), depends_on = [depnode], nodename = 'True Case')
    else:
        adageobj.dag.addTask(second_option.s(), depends_on = [depnode], nodename = 'False Case')

def main():
    adageobj = adage.adageobject()

    prepare_node    = adageobj.dag.addTask(boolean_task.s(), nodename = 'bool')
    adageobj.rules = [ Rule(node_done.s(nodename = 'bool'), schedule_if_else.s(depnode = 'bool')) ]

    adage.rundag(adageobj, default_trackers = True, trackevery = 5)

if __name__=='__main__':
    main()
