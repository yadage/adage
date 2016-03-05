import time
import os
import shutil
import subprocess
import dagstate
import datetime        
import networkx as nx
import adage.visualize as viz

class GifTracker(object):
    def __init__(self,gifname,workdir,frames = 20):
        self.gifname = gifname
        self.workdir = workdir
        self.frames = frames
        
    def initialize(self,dag):
        pass
        
    def track(self,dag):
        pass
        
    def finalize(self,dag):
        if os.path.exists(self.workdir):
            shutil.rmtree(self.workdir)
        os.makedirs(self.workdir)
        for i in range(self.frames+1):
            viz.print_dag(dag,'dag_{:02}'.format(i),self.workdir,time = i/float(self.frames))
        subprocess.call('convert -delay 50 $(ls {}/*.png|sort) {}'.format(self.workdir,self.gifname),shell = True)
        shutil.rmtree(self.workdir)
                
class TextSnapShotTracker(object):
    def __init__(self,logfilename,mindelta):
        self.logfilename = logfilename
        self.mindelta = mindelta
        self.last_update = None

    def initialize(self,dag):
        if not os.path.exists(os.path.dirname(self.logfilename)):
            os.makedirs(os.path.dirname(self.logfilename))
        with open(self.logfilename,'w') as logfile:
            timenow = datetime.datetime.now().isoformat()
            logfile.write('========== ADAGE LOG BEGIN at {} ==========\n'.format(timenow))
        self.update(dag)

    def track(self,dag):
        now = time.time()
        if not self.last_update or (now-self.last_update) > self.mindelta:
            self.last_update = now
            self.update(dag)

    def update(self,dag):
        with open(self.logfilename,'a') as logfile:
            logfile.write('---------- snapshot at {}\n'.format(datetime.datetime.now().isoformat()))
            for node in nx.topological_sort(dag):
                nodeobj = dag.getNode(node)
                logfile.write('name: {} obj: {} submitted: {}\n'.format(nodeobj.name,nodeobj,nodeobj.submitted))
    def finalize(self,dag):
        with open(self.logfilename,'a') as logfile:
            self.update(dag)
            timenow = datetime.datetime.now().isoformat()
            logfile.write('========== ADAGE LOG END at {} ==========\n'.format(timenow))
        
class SimpleReportTracker(object):
    def __init__(self,log):
        self.log = log
    def initialize(self,dag):
        pass
    def track(self,dag):
        pass
    def finalize(self,dag):
        successful, failed, notrun = 0,0,0
        for node in dag.nodes():
            nodeobj = dag.getNode(node)
            if dagstate.node_status(nodeobj):
                successful+=1
            if dagstate.node_ran_and_failed(nodeobj):
                failed+=1
                self.log.error("node: {} failed. reason: {}".format(nodeobj,nodeobj.backend.fail_info(nodeobj.result)))
            if dagstate.upstream_failure(dag,nodeobj):
                notrun+=1
        self.log.info('successful: {} | failed: {} | notrun: {} | total: {}'.format(successful,failed,notrun,len(dag.nodes())))
        
