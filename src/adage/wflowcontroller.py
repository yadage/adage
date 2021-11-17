import adage.controllerutils as ctrlutils
import logging
import adage.nodestate as nodestate

log = logging.getLogger(__name__)

class BaseController(object):

    def __init__(self, adageobj, backend = None):
        '''
        :param backend: the desired backend to which to submit nodes
        '''
        self._backend  = None
        self._adageobj = None

        self.backend  = backend
        self.adageobj = adageobj

    @property
    def adageobj(self):
        return self._adageobj

    @adageobj.setter
    def adageobj(self,adageobj):
        self._adageobj = adageobj
        if self.backend:
            ctrlutils.connect_backend(self.adageobj,self.backend)

    @property
    def backend(self):
        return self._backend

    @backend.setter
    def backend(self,backend):
        self._backend = backend
        if self.adageobj:
            ctrlutils.connect_backend(self.adageobj,backend)

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
        return ctrlutils.applicable_rules(self.adageobj)

    def submittable_nodes(self):
        '''
        :return: a list of nodes with sucessfull and completed upstream
        '''
        return ctrlutils.submittable_nodes(self.adageobj)

    def finished(self):
        '''
        :return: boolean indicating if nodes or rules are still left to be submitted/applied
        '''
        return not ctrlutils.nodes_left_or_rule_applicable(self.adageobj)

    def successful(self):
        '''
        :return: boolean indicating workflow execution was successful
        '''
        if not self.finished(): #will sync backend here
            return False
        failed = any(self.adageobj.dag.getNode(x).state == nodestate.FAILED for x in self.adageobj.dag.nodes())
        return (not failed)

    def validate(self):
        '''
        :return: validates internal execution order of workflow
        '''
        if not ctrlutils.validate_finished_dag(self.adageobj.dag):
            return False
        return True

    def sync_backend(self):
        '''
        :return: synchronize with backend to update workflow state
        '''
        return ctrlutils.sync_state(self.adageobj,self.backend)
