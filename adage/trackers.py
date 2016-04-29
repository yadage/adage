import time
import os
import shutil
import json
import subprocess
import dagstate
import nodestate
from datetime  import datetime
import networkx as nx
import adage.visualize as viz

class JSONDumpTracker(object):
    def __init__(self,dumpname):
        self.dumpname = dumpname

    def initialize(self,adageobj):
        pass
        
    def track(self,adageobj):
        pass
    
    def finalize(self,adageobj):
        dag, rules = adageobj.dag, adageobj.rules
        data = {'dag':None, 'rules':None}

        data['rules'] = {'nrules':len(rules)}
        data['dag'] = {'nodes':[]}
        for node in nx.topological_sort(dag):
            nodeobj = dag.getNode(node)
            nodeinfo = {
                'id':nodeobj.identifier,
                'name':nodeobj.name,
                'dependencies':dag.predecessors(nodeobj.identifier),
                'state':str(nodeobj.state),
                'timestamps':{
                    'defined': nodeobj.define_time,
                    'submit': nodeobj.submit_time,
                    'ready by': nodeobj.ready_by_time
                }
            }
            data['dag']['nodes']+=[nodeinfo]
        with open(self.dumpname,'w') as dumpfile:
            json.dump(data,dumpfile)

class GifTracker(object):
    def __init__(self,gifname,workdir,frames = 20):
        self.gifname = gifname
        self.workdir = workdir
        self.frames = frames
        
    def initialize(self,adageobj):
        pass
        
    def track(self,adageobj):
        pass
        
    def finalize(self,adageobj):
        if os.path.exists(self.workdir):
            shutil.rmtree(self.workdir)
        os.makedirs(self.workdir)
        for i in range(self.frames+1):
            viz.print_dag(adageobj.dag,'dag_{:02}'.format(i),self.workdir,time = i/float(self.frames))
        subprocess.call('convert -delay 50 $(ls {}/*.png|sort) {}'.format(self.workdir,self.gifname),shell = True)
        shutil.rmtree(self.workdir)
                
class TextSnapShotTracker(object):
    def __init__(self,logfilename,mindelta):
        self.logfilename = logfilename
        self.mindelta = mindelta
        self.last_update = None

    def initialize(self,adageobj):
        if not os.path.exists(os.path.dirname(self.logfilename)):
            os.makedirs(os.path.dirname(self.logfilename))
        with open(self.logfilename,'w') as logfile:
            timenow = datetime.now().isoformat()
            logfile.write('========== ADAGE LOG BEGIN at {} ==========\n'.format(timenow))
        self.update(adageobj)

    def track(self,adageobj):
        now = time.time()
        if not self.last_update or (now-self.last_update) > self.mindelta:
            self.last_update = now
            self.update(adageobj)

    def update(self,adageobj):
        dag = adageobj.dag
        with open(self.logfilename,'a') as logfile:
            logfile.write('---------- snapshot at {}\n'.format(datetime.now().isoformat()))
            for node in nx.topological_sort(dag):
                nodeobj = dag.getNode(node)
                submitted = nodeobj.submit_time is not None
                logfile.write('name: {} obj: {} submitted: {}\n'.format(
                        nodeobj.name,
                        nodeobj,
                        submitted
                    )
                )
    def finalize(self,adageobj):
        with open(self.logfilename,'a') as logfile:
            self.update(adageobj)
            timenow = datetime.now().isoformat()
            logfile.write('========== ADAGE LOG END at {} ==========\n'.format(timenow))

        
class SimpleReportTracker(object):
    def __init__(self,log,mindelta):
        self.log = log
        self.mindelta = mindelta
        self.last_update = None
        
    def initialize(self,adageobj):
        pass

    def track(self,adageobj):
        now = time.time()
        if not self.last_update or (now-self.last_update) > self.mindelta:
            self.last_update = now
            self.update(adageobj)
    
    def finalize(self,adageobj):
        self.update(adageobj)

    def update(self,adageobj):
        dag, rules = adageobj.dag, adageobj.rules
        successful, failed, running, notrun = 0, 0, 0, 0
        for node in dag.nodes():
            nodeobj = dag.getNode(node)
            if nodeobj.state == nodestate.RUNNING:
                running += 1
            if dagstate.node_status(nodeobj):
                successful+=1
            if dagstate.node_ran_and_failed(nodeobj):
                failed+=1
                self.log.error("node: {} failed. reason: {}".format(nodeobj,nodeobj.backend.fail_info(nodeobj.resultproxy)))
            if dagstate.upstream_failure(dag,nodeobj):
                notrun+=1
        self.log.info('successful: {} | failed: {} | running: {}| notrun: {} | total: {} | rules: {}'.format(
            successful,failed,running,notrun,len(dag.nodes()),len(rules)))
        
