import os
import shutil
import time
import random
from dagger import daggertask

import logging

log = logging.getLogger(__name__)

#dummy function to make stuff last randomly a bit longer
def chill():
  time.sleep(2+5*random.random())

@daggertask
def prepare(workdir):
  if os.path.exists(workdir):
    shutil.rmtree(workdir)
  os.makedirs(workdir)
  chill()

@daggertask
def download(workdir):
  #let's say we have 4 files
  filelist = []
  for i in range(4):
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

  if random.random() < 0.1:
    log.error('ERROR! in workdir {}'.format(workdir))
    raise IOError

  return hepmcfilename


@daggertask
def mcviz(hepmcfile):

  svgfilename = '{}/mcviz.svg'.format(os.path.dirname(hepmcfile))
  open(svgfilename, 'a').close()

  log.info('running mcviz on : {} -> {}'.format(hepmcfile,svgfilename))

  chill()

  return svgfilename

@daggertask
def rivet(workdir,hepmcfiles):
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


