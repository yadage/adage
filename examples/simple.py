#!/usr/bin/env python

import logging
import adage
from adage import adagetask, functorize, mknode,Rule, mknode, mk_dag
from celery import Celery
from adage.backends import CeleryBackend, DummyBackend, MultiProcBackend



logging.basicConfig(level = logging.INFO)


@adagetask
def mytask(one):
  import time
  time.sleep(4)
  print one

@adagetask
def tofail(one):
  print one
  print 'failing'
  raise RuntimeError

@functorize
def predicate(dag):
  return True
  
@functorize
def rulebody(dag):
  mknode(dag,mytask.s(2), depends_on = [dag.getNode(dag.nodes()[0])])

  
app = Celery('simple', broker = 'redis://', backend = 'redis://')

def main():
  
  dag = mk_dag()

  one = mknode(dag,mytask.s(1))
  two = mknode(dag,mytask.s(3), depends_on = [one])
  
  backend = MultiProcBackend(2)
  
  rules = [Rule(predicate.s(),rulebody.s())]
  
  try:
    adage.rundag(dag,rules, backend = backend, track = True, workdir = 'simpleTrack')
  except RuntimeError:
    print '===> ERROR'

if __name__ == '__main__':
  main()