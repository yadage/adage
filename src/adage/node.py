import time
import uuid
import logging

import adage.nodestate as nodestate

log = logging.getLogger(__name__)

class Node(object):
    def __init__(self,name,task,identifier = None, define_time = None, result = None):
        self.identifier = identifier or str(uuid.uuid4())
        self.name = name
        self.task = task

        # the timestamps
        self.define_time   = define_time or time.time()
        self.submit_time   = None
        self.ready_by_time = None

        # backend to update state against
        self.backend = None

        # relevant state data
        self.resultproxy = None
        self._result = result
        self._state = nodestate.DEFINED

    def __repr__(self):
        return '<Node name: {} id: {} state: {}>'.format(self.name,self.identifier,self.state)

    def update_state(self,backend = None):
        #if we do not have a result object
        #that means it's not submitted yet
        if not self.resultproxy:
            self._state = nodestate.DEFINED
            return

        backend = self.backend or backend
        if not backend:
            raise RuntimeError('no backend to update state against')

        #if we have a resultobject
        #but the result is not ready
        #the node is still running
        if not backend.ready(self.resultproxy):
            self._state = nodestate.RUNNING
            return

        #if it's ready it's either successful
        #or failed
        if backend.successful(self.resultproxy):
            self._state  = nodestate.SUCCESS
            self._result = backend.result(self.resultproxy)
        else:
            self._state = nodestate.FAILED

        #it's ready so set time stamp it not already set
        if not self.ready_by_time:
            self.ready_by_time = time.time()
            log.info('node ready %s',self)

    @property
    def result(self):
        if self._result is None:
            log.warning(
                'result requested but it is None proxy: %s, backend: %s',
                self.resultproxy, self.backend
            )
        return self._result

    @property
    def state(self):
        return self._state

    def ready(self):
        return self.state in [nodestate.SUCCESS, nodestate.FAILED]

    def successful(self):
        return self.state == nodestate.SUCCESS
