import adage
import adage.dagstate
from adage import adageop, Rule

#import some task functions that we'd like to run
from physicstasks import prepare, download, rivet, pythia, plotting, mcviz

import logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)
@adageop
def node_done(adageobj,nodename):
    #we can only run pythia once the donwload is done and we know hoe many LHE files we have
    download_node = adageobj.dag.getNodeByName(nodename)
    if download_node:
        return adage.dagstate.node_status(download_node)
    return False

@adageop
def schedule_pythia(adageobj):

    download_node = adageobj.dag.getNodeByName('download')
    lhefiles = download_node.result

    #let's run pythia on these LHE files
    pythia_nodes = [adageobj.dag.addTask(pythia.s(lhefilename = lhe),nodename = 'pythia', depends_on = [download_node]) for lhe in lhefiles]

    # we already know what the pythia result will look like so we don't need to wait for the nodes to run
    # to schedule them
    hepmcfiles        = [x.rsplit('.lhe')[0]+'.hepmc' for x in lhefiles]

    adageobj.dag.addTask(mcviz.s(hepmcfile = hepmcfiles[0]),nodename = 'mcviz',  depends_on = pythia_nodes[0:1])

    #Rivet and then produce some plots.
    rivet_node        = adageobj.dag.addTask(rivet.s(workdir = 'here', hepmcfiles = hepmcfiles), nodename = 'rivet', depends_on = pythia_nodes)
    adageobj.dag.addTask(plotting.s(workdir = 'here', yodafile = 'Rivet.yoda'),nodename = 'plotting', depends_on = [rivet_node])

def build_initial_dag():
    adageobj = adage.adageobject()

    prepare_node    = adageobj.dag.addTask(prepare.s(workdir = 'here'), nodename = 'prepare')
    adageobj.dag.addTask(download.s(workdir = 'here'), depends_on = [prepare_node], nodename = 'download')

    #possible syntax that could be nice using partial function execution
    #    download_node = do(download.s(workdir = 'here'), depends_on = [prepare_node], nodename = 'download')

    adageobj.rules = [ Rule(node_done.s(nodename = 'download'), schedule_pythia.s()) ]
    return adageobj

def talkative_decider():
    log.info('ok we started and are now waiting for our first data')
    data = yield
    while True:
        log.info('we received some new data: %s and we will make a decision now',data)
        value = True
        log.info('ok.. decision reached.. yielding with this decision %s',value)
        data = yield value

def main():
    adageobj = build_initial_dag()
    t = talkative_decider()
    t.next()
    adage.rundag(adageobj, default_trackers = True, trackevery = 5)

if __name__=='__main__':
    main()
