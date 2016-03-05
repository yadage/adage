import nodestate
import time


class Node(object):
    def __init__(self,identifier,name,task):
        self.identifier = identifier
        self.name = name
        self.submit_time = None
        self.ready_by_time = None
        self.the_state = nodestate.DEFINED
        self.define_time = time.time()        
        self.task = task
        self.the_result = None
        self.the_backend = None

    def __repr__(self):
        return '<Node name: {} id: {} state: {}>'.format(self.name,self.identifier,self.state())

    def update_state(self):
        #if we do not have a result object
        #that means it's not submitted yet
        if not self.the_result:
            self.the_state = nodestate.DEFINED
            return
        
        #if we have a resultobject
        #but the result is not ready
        #the node is still running
        if not self.ready():
            self.the_state = nodestate.RUNNING
            return
            
        #if it's ready it's either successful
        #or failed
        if self.successful():
            self.the_state = nodestate.SUCCESS
            return
        else:
            self.the_state = nodestate.FAILED
            return

    def state(self):
        self.update_state()
        return self.the_state

    @property
    def backend(self):
        return self.the_backend
    
    @backend.setter
    def backend(self,backend):
        self.the_backend = backend
    
    @property
    def submitted(self):
        return self.submit_time
        
    @submitted.setter
    def submitted(self,time):
        self.the_state = nodestate.RUNNING
        self.submit_time = time
        
    @property
    def result(self):
        return self.the_result
        
    @result.setter
    def result(self,result):
        self.the_result = result
    
    def ready(self):
        if not self.the_result:
            return False

        ready = self.backend.ready(self.the_result)
        if not ready:
            return False
        if not self.ready_by_time:
            self.ready_by_time = time.time()
        return True

    def result_of(self):
        return self.backend.result_of(self.the_result)
        
    def successful(self):
        if not self.the_result:
            return False
        return self.backend.successful(self.the_result)
