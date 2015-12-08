#!/usr/bin/env python

import adage
from adage import adagetask, rulefunc,mknode

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

@rulefunc
def RulePredicate(otherkw, dag):
  print "PREDICATE"
  print otherkw
  return True
  
@rulefunc
def RuleApply(otherkw, dag):
  print "APPLY"
  print otherkw
  adage.mknode(dag,mytask.s(4,5,6), depends_on = [dag.node[0]])

  
import logging
logging.basicConfig(level = logging.INFO)

from celery import Celery
app = Celery('simple', broker = 'redis://', backend = 'redis://')

def main():
  
  dag = adage.mk_dag()

  one = adage.mknode(dag,mytask.s(1))
  two = adage.mknode(dag,mytask.s(2), depends_on = [one] )
  tre = adage.mknode(dag,mytask.s(3), depends_on = [one] )
  vor = adage.mknode(dag,tofail.s(4), depends_on = [tre] )
  fiv = adage.mknode(dag,tofail.s(5), depends_on = [vor] )
  six = adage.mknode(dag,tofail.s(6), depends_on = [fiv] )
  sev = adage.mknode(dag,tofail.s(7), depends_on = [six,two] )
  ait = adage.mknode(dag,mytask.s(8), depends_on = [two,tre])
  
  from adage.backends import CeleryBackend, DummyBackend, MultiProcBackend
  # backend = CeleryBackend(app)
  # backend = DummyBackend()
  backend = MultiProcBackend(2)
  
  rules = []
  
  try:
    adage.rundag(dag,rules, backend = backend, track = True, workdir = 'simpleTrack')
  except RuntimeError:
    print '===> ERROR'

if __name__ == '__main__':
  main()