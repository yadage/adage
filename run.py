from example import prepare, download, rivet, pythia, plotting

import dagger.dag
from dagger.dag import rulefunc,mknode,signature,get_node_by_name,result_of

@rulefunc
def download_done(dag):
  download_node = get_node_by_name(dag,'download')
  if download_node:
      return dagger.dag.node_status(dag,download_node['nodenr'])
  return False
  
@rulefunc
def schedule_pythia(dag):
  download_node = get_node_by_name(dag,'download')

  lhefiles = result_of(download_node)

  pythia_nodes = [mknode(dag,pythia.s(lhefilename = lhe), depends_on = [download_node]) for lhe in lhefiles]

  hepmcfiles    = [x.rsplit('.lhe')[0]+'.hepmc' for x in lhefiles]
  rivet_node    = mknode(dag,rivet.s(workdir = 'here', hepmcfiles = hepmcfiles), depends_on = pythia_nodes)
  plotting_node = mknode(dag,plotting.s(workdir = 'here', yodafile = 'Rivet.yoda'), depends_on = [rivet_node])
    
def build_dag():
  dag = dagger.dag.mk_dag()

  prepare_node  = mknode(dag,prepare.s(workdir = 'here'))
  download_node = mknode(dag,download.s(workdir = 'here'), depends_on = [prepare_node], nodename = 'download')

  #possible syntax that could be nice using partial function execution
  #  download_node = do(download.s(workdir = 'here'), depends_on = [prepare_node], nodename = 'download')

  rules =  [ (download_done.s(), schedule_pythia.s()) ]
  return dag,rules
  
def main():
  dag,rules = build_dag()
  dagger.dag.rundag(dag,rules)

if __name__=='__main__':
  main()
  