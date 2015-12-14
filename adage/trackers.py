import time
import adage.visualize as viz
import os
import shutil
import subprocess
import dagstate

class GifTracker(object):
    def __init__(self,gifname,workdir,mindelta):
        self.last_update = None
        self.gifname = gifname
        self.workdir = workdir
        self.mindelta = mindelta

    def initialize(self,dag):
        if os.path.exists(self.workdir):
            shutil.rmtree(self.workdir)
        os.makedirs(self.workdir)
        self.update(dag)
        
    def track(self,dag):
        now = time.time()
        if not self.last_update or (now-self.last_update) > self.mindelta:
            self.last_update = now
            self.update(dag)

    def finalize(self,dag):
        self.update(dag)
        subprocess.call('convert -delay 50 $(ls {}/*.png|sort) {}'.format(self.workdir,self.gifname),shell = True)
        shutil.rmtree(self.workdir)

    def update(self,dag):
        viz.print_next_dag(dag,self.workdir)
        
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
        
