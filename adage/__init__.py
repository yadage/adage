import os
import time
import logging
import trackers

from adage.decorators import adageop, adagetask, Rule
from adage.adageobject import adageobject
from adage.pollingexec import adage_coroutine
from adage.wflowcontroller import InMemoryController

#silence pyflakes
assert adageop
assert adagetask
assert Rule
assert adageobject

log = logging.getLogger(__name__)


def yes_man(messagestring = 'received %s and %s'):
    '''trivial decision function that just returns True always'''
    # we yield until we receive some data via send()
    data = yield
    while True:
        log.debug(messagestring,data)
        #we yield True and wait again to receive some data
        value = True
        data = yield value

def trackprogress(trackerlist,adageobj, method = 'track'):
    '''
    track adage workflow state using a list of trackers

    :param trackerlist: the stackers which should inspect the workflow
    :param adageobj: the adage workflow object
    :param method: tracking method to call. Must be one of ``initialize``, ``track``, ``finalize``
    :return: None
    '''
    map(lambda t: getattr(t,method)(adageobj), trackerlist)

def rundag(adageobj,
           backend = None,
           extend_decider = None,
           submit_decider = None,
           update_interval = 0.01,
           loggername = None,
           trackevery = 1,
           workdir = None,
           default_trackers = True,
           additional_trackers = None
    ):
    '''
    Main adage entrypoint. It's a convenience wrapper around the main adage coroutine loop and
    sets up the backend, logging, tracking (for GIFs, Text Snapshots, etc..) and possible interactive
    hooks into the coroutine

    :param adageobj: the adage workflow object
    :param backend: the task execution backend to which to submit node tasks
    :param extend_decider: decision coroutine to deal with whether to extend the workflow graph
    :param submit_decider: decision coroutine to deal with whether to submit node tasks
    :param update_interval: minimum looping interval for main adage loop 
    :param loggername: python logger to use
    :param trackevery: tracking interval for default simple report tracker
    :param workdir: workdir for default visual tracker
    :param default_trackers: whether to enable default trackers (simple report, gif visualization, text snapshot)
    :param additional_trackers: list of any additional tracking objects
    '''


    if loggername:
        global log
        log = logging.getLogger(loggername)

    ## funny behavior of multiprocessing Pools means that
    ## we can not have backendsubmit = multiprocsetup(2)    in the function sig
    ## so we only initialize them here
    if not backend:
        from backends import MultiProcBackend
        backend = MultiProcBackend(2)

    ## prepare the decision coroutines...
    if not extend_decider:
        extend_decider = yes_man('say yes to graph extension by rule: %s state: %s')
        extend_decider.next()

    if not submit_decider:
        submit_decider = yes_man('say yes to node submission of: %s%s')
        submit_decider.next()

    ## prepare tracking objects
    trackerlist = []
    if default_trackers:
        if not workdir:
            workdir = os.getcwd()
        trackerlist  = [trackers.SimpleReportTracker(log,trackevery)]
        trackerlist += [trackers.GifTracker(gifname = '{}/workflow.gif'.format(workdir), workdir = '{}/track'.format(workdir))]
        trackerlist += [trackers.TextSnapShotTracker(logfilename = '{}/adagesnap.txt'.format(workdir), mindelta = trackevery)]
    if additional_trackers:
        trackerlist += additional_trackers

    ## prepare the controller
    controller = InMemoryController(adageobj, backend)

    log.info('preparing adage coroutine.')
    coroutine = adage_coroutine(extend_decider,submit_decider)
    coroutine.next() #prime the coroutine....
    coroutine.send(controller)
    log.info('starting state loop.')

    try:
        trackprogress(trackerlist, controller.adageobj, method = 'initialize')
        for controller in coroutine:
            trackprogress(trackerlist, controller.adageobj)
            time.sleep(update_interval)
    except:
        log.exception('some weird exception caught in adage process loop')
        raise
    finally:
        trackprogress(trackerlist, controller.adageobj, method = 'finalize')

    log.info('adage state loop done.')

    if not controller.validate():
        raise RuntimeError('DAG execution not validating')
    log.info('execution valid. (in terms of execution order)')

    if not controller.successful():
        log.error('raising RunTimeError due to failed jobs')
        raise RuntimeError('DAG execution failed')

    log.info('workflow completed successfully.')
