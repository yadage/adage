import adage
import adage.dagstate
from adage import adagetask, functorize, Rule,mknode

import random
import logging
import time
log = logging.getLogger(__name__)
logging.basicConfig(level = logging.DEBUG)

@adagetask
def pdfproducer(name):
  time.sleep(2+10*random.random())
  open('{}.pdf'.format(name), 'a').close()

@adagetask
def variableoutput():
  log.info('determining number of pdf jobs to launch')
  pdfjobs = random.randint(1,2)
  return pdfjobs
  
@adagetask
def mergepdf():
  log.info('merging...')
  time.sleep(2+1*random.random())
  
  open('merged.pdf','a').close()
  

@functorize
def variable_nodes_done(varnodes,dag):
  #ready if we have a finished variable node that has no ancestors
  return all([adage.dagstate.node_status(n) for n in varnodes])

@functorize
def schedule_pdf(fixednodes, varnodes,dag):
  log.info('scheduling pdf')
  allpdfjobs = fixednodes
  for node in varnodes:
    npdf = node.result_of()
    allpdfjobs += [mknode(dag,
                          task = pdfproducer.s(name = 'fromvar_{}_{}'.format(node.name,i)),
                          depends_on = [node]) for i in range(npdf)]
  
  mknode(dag,task = mergepdf.s(),depends_on = allpdfjobs)
  
def main():
  dag = adage.mk_dag()
  
  fix0 = mknode(dag,task = pdfproducer.s(name = 'fixed'))
  var1 = mknode(dag,nodename = 'variable1', task = variableoutput.s())
  var2 = mknode(dag,nodename = 'variable2', task = variableoutput.s())

  varnodes = [var1,var2]

  rules = []
  rules += [
    Rule(variable_nodes_done.s(varnodes),schedule_pdf.s([fix0],varnodes))
  ]

  adage.rundag(dag,rules,track = True, workdir = 'bla')

if __name__=='__main__':
  main()