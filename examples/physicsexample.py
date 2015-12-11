import adage
from adage import functorize,Rule,mknode
import adage.dagstate

#import some task functions that we'd like to run
from physicstasks import prepare, download, rivet, pythia, plotting, mcviz

import logging
logging.basicConfig(level=logging.INFO)

@functorize
def download_done(dag):
  #we can only run pythia once the donwload is done and we know hoe many LHE files we have
  download_node = dag.getNodeByName('download')
  if download_node:
      return adage.dagstate.node_status(download_node)
  return False
  
@functorize
def schedule_pythia(dag):
  
  download_node = dag.getNodeByName('download')
  lhefiles = download_node.result_of()

  #let's run pythia on these LHE files
  pythia_nodes = [mknode(dag,pythia.s(lhefilename = lhe), depends_on = [download_node]) for lhe in lhefiles]

  # we already know what the pythia result will look like so we don't need to wait for the nodes to run
  # to schedule them
  hepmcfiles    = [x.rsplit('.lhe')[0]+'.hepmc' for x in lhefiles]

  mcviz_node    = mknode(dag,mcviz.s(hepmcfile = hepmcfiles[0]), depends_on = pythia_nodes[0:1])

  #Rivet and then produce some plots.
  rivet_node    = mknode(dag,rivet.s(workdir = 'here', hepmcfiles = hepmcfiles), depends_on = pythia_nodes)
  plotting_node = mknode(dag,plotting.s(workdir = 'here', yodafile = 'Rivet.yoda'), depends_on = [rivet_node])
    
def build_initial_dag():
  dag = adage.mk_dag()

  prepare_node  = mknode(dag,prepare.s(workdir = 'here'))
  download_node = mknode(dag,download.s(workdir = 'here'), depends_on = [prepare_node], nodename = 'download')

  #possible syntax that could be nice using partial function execution
  #  download_node = do(download.s(workdir = 'here'), depends_on = [prepare_node], nodename = 'download')

  rules =  [ Rule(download_done.s(), schedule_pythia.s()) ]
  return dag,rules
  

def main():
  dag,rules = build_initial_dag()
  adage.rundag(dag,rules, track = True, trackevery = 5)

if __name__=='__main__':
  main()