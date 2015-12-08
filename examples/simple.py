#!/usr/bin/env python

import adage
from adage import adagetask, functorize, mknode,Rule

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
def predicate(otherkw, dag):
  return True
  
@functorize
def rulebody(otherkw, dag):
  adage.mknode(dag,mytask.s(2), depends_on = [dag.node[dag.nodes()[0]]['nodeobj']])

  
import logging
logging.basicConfig(level = logging.INFO)

from celery import Celery
app = Celery('simple', broker = 'redis://', backend = 'redis://')

def main():
  
  dag = adage.mk_dag()

  one = adage.mknode(dag,mytask.s(1))
  
  from adage.backends import CeleryBackend, DummyBackend, MultiProcBackend
  backend = MultiProcBackend(2)
  
  rules = [Rule(predicate.s(otherkw = 'what'),rulebody.s(otherkw = 'THAT'))]
  
  try:
    adage.rundag(dag,rules, backend = backend, track = True, workdir = 'simpleTrack')
  except RuntimeError:
    print '===> ERROR'

if __name__ == '__main__':
  main()