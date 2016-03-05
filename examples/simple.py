#!/usr/bin/env python
import time
import os
import logging
import adage
import adage.nodestate
import adage.backends
from adage import adagetask, functorize, mknode,Rule, mk_dag
try:
    from celery import Celery
except ImportError:
    pass

logging.basicConfig(level = logging.INFO)
log = logging.getLogger(__name__)

@adagetask
def mytask(one):
    log.info('sleeping for {}, pid: {}'.format(one,os.getpid()))
    time.sleep(one)

@adagetask
def tofail(one):
    print 'failing'
    raise RuntimeError

@functorize
def predicate(dag,depnode):
    return depnode.state() == adage.nodestate.SUCCESS
    
@functorize
def rulebody(dag,depnode):
    mapnodes = []
    for i in range(6):
        mapnodes += [mknode(dag,mytask.s(3*i), depends_on = [depnode], nodename = 'map')]
    mknode(dag,mytask.s(3), depends_on = mapnodes, nodename = 'reduce')


def main():
    backend = adage.backends.MultiProcBackend(2)
    
    dag = mk_dag()
    one = mknode(dag,mytask.s(5), nodename = 'first')
    two = mknode(dag,mytask.s(3), depends_on = [one], nodename = 'second')
    
    rules = [Rule(predicate.s(depnode = two),rulebody.s(depnode = two))]
    
    try:
        adage.rundag(dag,rules, backend = backend, track = True, workdir = 'simpleTrack', trackevery = 10)
    except RuntimeError:
        print '===> ERROR'

if __name__ == '__main__':
    main()