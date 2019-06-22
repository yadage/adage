import os
import time
import logging

from adage.decorators import adageop, adagetask, Rule
from adage.adageobject import adageobject
from adage.pollingexec import setup_polling_execution
from adage.wflowcontroller import BaseController

import adage.trackers as trackers

#silence pyflakes
assert adageop
assert adagetask
assert Rule
assert adageobject

log = logging.getLogger(__name__)

def trackprogress(trackerlist, controller, method = 'track'):
    '''
    track adage workflow state using a list of trackers

    :param trackerlist: the stackers which should inspect the workflow
    :param adageobj: the adage workflow object
    :param method: tracking method to call. Must be one of ``initialize``, ``track``, ``finalize``
    :return: None
    '''
    for t in trackerlist:
        getattr(t,method)(controller.adageobj)

def run_polling_workflow(controller, coroutine, update_interval, trackerlist = None, maxsteps = None):
    '''
    run the polling-style workflow by periodically checkinng if a graph can be extended or any noted be submitted
    runs validation

    :param controller: the workflow controller
    :param coroutine: the adage coroutine to step through the workflow
    :param update_interval: time interval between workflow ticks
    :param trackerlist

    :return: None
    :raises RuntimeError: if graph is finished and graph validation failed or workflow is failed (nodes unsuccessful)
    '''

    coroutine.send(controller)
    log.info('starting state loop.')

    try:
        trackprogress(trackerlist, controller, method = 'initialize')
        for stepnum, controller in enumerate(coroutine):
            log.debug('yielded a controller step. Tracking the workflow.')
            trackprogress(trackerlist, controller)
            if maxsteps and (stepnum+1 == maxsteps):
                log.info('reached number of maximum iterations ({})'.format(maxsteps))
                return
            log.debug('Tracking done.')
            time.sleep(update_interval)
    except:
        log.exception('some weird exception caught in adage process loop')
        raise
    finally:
        trackprogress(trackerlist, controller, method = 'finalize')

    log.info('adage state loop done.')

    if not controller.validate():
        raise RuntimeError('DAG execution not validating')
    log.info('execution valid. (in terms of execution order)')

    log.info('workflow completed successfully.')

def default_trackerlist(gif_workdir = None, text_loggername = __name__, texttrack_delta = 1):
    workdir = gif_workdir or os.getcwd()
    trackerlist  = [trackers.SimpleReportTracker(text_loggername, texttrack_delta)]
    trackerlist += [trackers.GifTracker(gifname = '{}/workflow.gif'.format(workdir), workdir = '{}/track'.format(workdir))]
    trackerlist += [trackers.TextSnapShotTracker(logfilename = '{}/adagesnap.txt'.format(workdir), mindelta = texttrack_delta)]
    return trackerlist

def rundag(adageobj = None,
           backend = None,
           extend_decider = None,
           submit_decider = None,
           finish_decider = None,
           recursive_updates = True,
           update_interval = 0.01,
           loggername = __name__,
           trackevery = 1,
           workdir = None,
           default_trackers = True,
           additional_trackers = None,
           controller = None,
           maxsteps = None):
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
    :param controller: optional external controller (instead of adageobj parameter)
    :param maxsteps: maximum number of steps in polling-style workflow
    :param recursive_updates: recurse if any rule has been applied
    '''
    if loggername:
        global log
        log = logging.getLogger(loggername)

    ## get primed coroutine for polling-style workflow execution
    # recursive_updates = False
    coroutine = setup_polling_execution(extend_decider, submit_decider, finish_decider, recursive_updates)

    ## prepare tracking objects
    trackerlist = default_trackerlist(workdir, loggername, trackevery) if default_trackers else []
    if additional_trackers:
        trackerlist += additional_trackers

    if adageobj:
        ## funny behavior of multiprocessing Pools means that
        ## we can not have backendsubmit = multiprocsetup(2)    in the function sig
        ## so we only initialize them here
        if not backend:
            from .backends import MultiProcBackend
            backend = MultiProcBackend(2)

        ## prep controller with backend
        controller = BaseController(adageobj, backend)

    run_polling_workflow(controller, coroutine, update_interval, trackerlist, maxsteps)
