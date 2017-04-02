import adage.controllerutils as ctrlutils
import logging
import nodestate

log = logging.getLogger(__name__)

class BaseController(object):
    '''
    standard workflow controller, that keeps workflow state in memory at all times without any disk I/O or
    database access.
    '''

    def __init__(self, backend = None):
        '''
        :param backend: the desired backend to which to submit nodes
        '''
        self.backend  = backend

    def submit_nodes(self, nodes):
        '''
        :param nodes: a list of nodes to submit to the backend. 

        Nodes are expected to be provided in same form as what the controller returns.
        '''
        ctrlutils.submit_nodes(nodes, self.backend)

    def apply_rules(self, rules):
        '''
        :param rules: a list of rules to to apply to the workflow graph

        Rules are expected to be provided in same form as what the controller returns.
        '''
        ctrlutils.apply_rules(self.adageobj, rules)

    def applicable_rules(self):
        '''
        :return: return a list of rules whose predicate is fulfilled
        '''
        self.sync_backend()
        return ctrlutils.applicable_rules(self.adageobj)

    def submittable_nodes(self):
        '''
        :return: a list of nodes with sucessfull and completed upstream
        '''
        self.sync_backend()
        return ctrlutils.submittable_nodes(self.adageobj)

    def finished(self):
        '''
        :return: boolean indicating if nodes or rules are still left to be submitted/applied    
        '''
        self.sync_backend() #so that we are up to date
        return not ctrlutils.nodes_left_or_rule_applicable(self.adageobj)

    def successful(self):
        '''
        :return: boolean indicating workflow execution was successful
        '''
        self.sync_backend()
        failed = any(self.adageobj.dag.getNode(x).state == nodestate.FAILED for x in self.adageobj.dag.nodes())
        return (not failed)

    def validate(self):
        '''
        :return: validates internal execution order of workflow
        '''
        if not ctrlutils.validate_finished_dag(self.adageobj.dag):
            return False

        # if self.adageobj.rules:
        #     log.warning('some rules were not applied.')

        return True

    def sync_backend(self):
        '''
        :return: synchronize with backend to update workflow state
        '''
        return ctrlutils.sync_state(self.adageobj)

class InMemoryController(BaseController):
    def __init__(self, adageobj, backend):
        self.adageobj = adageobj
        super(InMemoryController, self).__init__(backend)
