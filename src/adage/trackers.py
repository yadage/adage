import logging
import time
import os
import shutil
import json
import subprocess
from datetime  import datetime
import networkx as nx
import adage.visualize as viz
import adage.serialize as serialize
import adage.dagstate as dagstate
import adage.nodestate as nodestate

class JSONDumpTracker(object):
    def __init__(self,dumpname, serializer = serialize.DefaultAdageEncoder):
        self.serializer = serializer
        self.dumpname = dumpname

    def initialize(self,adageobj):
        pass

    def track(self,adageobj):
        pass

    def finalize(self,adageobj):
        with open(self.dumpname,'w') as dumpfile:
            json.dump(adageobj,dumpfile, cls = self.serializer)

class GifTracker(object):
    def __init__(self,gifname,workdir,frames = 40):
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
        subprocess.check_call('convert -delay 50 $(ls {}/*.png|sort) {}'.format(self.workdir,self.gifname),shell = True)
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
    def __init__(self, loggername, mindelta):
        self.log = logging.getLogger(loggername)
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
        dag, rules, applied = adageobj.dag, adageobj.rules, adageobj.applied_rules
        successful, failed, running, unsubmittable = 0, 0, 0, 0
        for node in dag.nodes():
            nodeobj = dag.getNode(node)
            if nodeobj.state == nodestate.RUNNING:
                running += 1
            if dagstate.node_status(nodeobj):
                successful+=1
            if dagstate.node_ran_and_failed(nodeobj):
                failed+=1
                reason = nodeobj.backend.fail_info(nodeobj.resultproxy) if nodeobj.backend else 'unknown'
                self.log.error("node: {} failed. reason: {}".format(nodeobj,reason))
            if dagstate.upstream_failure(dag,nodeobj):
                unsubmittable+=1
        self.log.info('unsubmittable: {unsubmittable} | submitted: {submitted} | successful: {successful} | failed: {failed} | total: {total} | open rules: {rules} | applied rules: {applied}'.format(
            successful = successful,
            failed = failed,
            submitted = running,
            unsubmittable = unsubmittable,
            total =  len(dag.nodes()),
            rules = len(rules),
            applied = len(applied)))
