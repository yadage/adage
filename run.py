from example import prepare, download, rivet, pythia, plotting

import dagger.dag
from dagger.dag import daggertask,rulefunc,mknode,signature,get_node_by_name,add_edge,result_of

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

  hepmcfiles = [x.rsplit('.lhe')[0]+'.hepmc' for x in lhefiles]
  rivet_node = mknode(dag, sig = rivet.s(workdir = 'here', hepmcfiles = hepmcfiles))

  plotting_node = mknode(dag, sig = plotting.s(workdir = 'here', yodafile = 'Rivet.yoda'))

  add_edge(dag,rivet_node,plotting_node)

  for lhe in lhefiles:
    lhe_node = mknode(dag, sig = pythia.s(lhefilename = lhe))
    
    add_edge(dag,download_node,lhe_node)
    add_edge(dag,lhe_node,rivet_node)
    
def build_dag():
  dag = dagger.dag.mk_dag()
  prepare_node = mknode(dag,sig = prepare.s(workdir = 'here'))

  download_node = mknode(dag,nodename = 'download', sig = download.s(workdir = 'here'))
  add_edge(dag,prepare_node,download_node)

  rules =  [ (download_done.s(), schedule_pythia.s()) ]

  return dag,rules
  
def main():
  dag,rules = build_dag()
  dagger.dag.rundag(dag,rules)

if __name__=='__main__':
  main()