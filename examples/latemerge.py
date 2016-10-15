import adage
import adage.dagstate
from adage import adagetask, adageop, Rule

import random
import logging
import time
log = logging.getLogger(__name__)
logging.basicConfig(level = logging.INFO)

@adagetask
def pdfproducer(name):
    time.sleep(2+10*random.random())
    open('{}.pdf'.format(name), 'a').close()

@adagetask
def variableoutput():
    log.info('determining number of pdf jobs to launch')
    pdfjobs = random.randint(1,2)
    time.sleep(2+10*random.random())
    return pdfjobs
    
@adagetask
def mergepdf():
    log.info('merging...')
    time.sleep(2+1*random.random())
    open('merged.pdf','a').close()
    

@adageop
def variable_nodes_done(varnodes,adageobj):
    #ready if we have a finished variable node that has no ancestors
    return all([adage.dagstate.node_status(n) for n in varnodes])

@adageop
def schedule_pdf(fixednodes, varnodes,adageobj):
    log.info('scheduling pdf')
    allpdfjobs = fixednodes
    for node in varnodes:
        npdf = node.result
        allpdfjobs += [ adageobj.dag.addTask(
                            pdfproducer.s(name = 'fromvar_{}_{}'.format(node.name,i)),
                            depends_on = [node],
                        ) for i in range(npdf)
                      ]
    adageobj.dag.addTask(mergepdf.s(),depends_on = allpdfjobs)
    
def main():
    adageobj = adage.adageobject()
    
    fix0 = adageobj.dag.addTask(pdfproducer.s(name = 'fixed'), nodename = 'fixed')
    var1 = adageobj.dag.addTask(variableoutput.s(), nodename = 'variable1')
    var2 = adageobj.dag.addTask(variableoutput.s(), nodename = 'variable2')

    varnodes = [var1,var2]

    adageobj.rules += [
        Rule(variable_nodes_done.s(varnodes),schedule_pdf.s([fix0],varnodes))
    ]

    adage.rundag(adageobj,default_trackers = True, workdir = 'bla')

if __name__=='__main__':
    main()