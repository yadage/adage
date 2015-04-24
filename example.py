import os
import shutil
import time
import random

import logging
logging.basicConfig(level=logging.INFO)

log = logging.getLogger(__name__)

#dummy function to make stuff last randomly a bit longer
def chill():
  time.sleep(2+5*random.random())

from dagger.dag import daggertask,rulefunc,mknode,signature
import dagger.dag

@daggertask
def prepare(workdir):
  if os.path.exists(workdir):
    shutil.rmtree(workdir)
  os.makedirs(workdir)
  chill()

@daggertask
def download(workdir):
  #let's say we have 3 files
  filelist = []
  for i in range(3):
    filename = 'inputfile_{}.lhe'.format('{}'.format(i).zfill(3))
    filename = '{}/{}'.format(workdir,filename)
    open(filename, 'a').close()
    filelist += [filename]
  
  chill()
  return filelist
  
@daggertask
def pythia(lhefilename):
  #let's say we have 10 files
  hepmcfilename = lhefilename.rsplit('.lhe')[0]+'.hepmc'
  log.info('running pythia: {} -> {}'.format(lhefilename,hepmcfilename))

  open(hepmcfilename, 'a').close()

  chill()

  if random.random() < 0.3:
    log.error('ERROR! in workdir {}'.format(workdir))
    raise IOError

  return hepmcfilename

@daggertask
def rivet(workdir,hepmcfiles):
  #let's say we have 10 files
  
  log.info('running rivet on these files: {}'.format(hepmcfiles))

  yodafilename = '{}/Rivet.yoda'.format(workdir)
  open(yodafilename, 'a').close()
  
  chill()
  return yodafilename

@daggertask
def plotting(workdir,yodafile):
  #let's say we have 10 files
  
  log.info('plotting stuff in yoda file')

  plotfilename = '{}/plots.pdf'.format(workdir)
  open(plotfilename, 'a').close()
  
  chill()
  return plotfilename

@rulefunc
def download_done(dag):
  for node in dag.nodes():
    if dag.node[node]['taskname'] == download.taskname:
      if dagger.dag.node_status(dag,node):
        return True
  return False
  
@rulefunc
def schedule_pythia(dag):
  download_node = None
  for node in dag.nodes():
    if dag.node[node]['taskname'] == download.taskname:
      download_node = node

  lhefiles = dag.node[download_node]['result'].get()


  hepmcfiles = [x.rsplit('.lhe')[0]+'.hepmc' for x in lhefiles]
  rivet_node = mknode(dag,nodename   = 'rivet',
                       taskname   =  rivet.taskname,
                       taskkwargs   =  {'workdir':'here','hepmcfiles':hepmcfiles}
                  )

  plotting_node = mknode(dag,nodename   = 'plotting',
                       taskname   =  plotting.taskname,
                       taskkwargs   =  {'workdir':'here','yodafile':'Rivet.yoda'}
                  )
  dag.add_edge(rivet_node['nodenr'],plotting_node['nodenr'])

  for lhe in lhefiles:
    lhe_node = mknode(dag,nodename   = 'pythia',
                         taskname   =  pythia.taskname,
                         taskkwargs   =  {'lhefilename':lhe}
                    )
    dag.add_edge(download_node,lhe_node['nodenr'])
    dag.add_edge(lhe_node['nodenr'],rivet_node['nodenr'])



def build_dag():
  dag = dagger.dag.mk_dag()
  prepare_node = mknode(dag,nodename   = 'prepare',
                       taskname   =  prepare.taskname,
                       taskkwargs   =  {'workdir':'here'}
                  )

  download_node = mknode(dag,nodename   = 'download',
                       taskname   =  download.taskname,
                       taskkwargs   =  {'workdir':'here'}
                  )
  dag.add_edge(prepare_node['nodenr'],download_node['nodenr'])

  rules =  [ (signature(download_done.rulename), signature(schedule_pythia.rulename)) ]

  return dag,rules
