import json
import logging

import adage.nodestate
import adage.adageobject
import adage.graph

log = logging.getLogger(__name__)

class DefaultAdageEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, adage.adageobject):
            return obj_to_json(obj)
        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, obj)

def noop_taskserializer(task):
    return 'unserializable_task'

def noop_proxyserializer(proxy):
    return 'unserializable_proxy'

def noop_ruleserializer(rule):
    return 'unserializable_rule'

def obj_to_json(adageobj, ruleserializer, nodeserializer):
    dag, rules, applied = adageobj.dag, adageobj.rules, adageobj.applied_rules
    data = {'dag':None, 'rules':None, 'applied':None}

    data['dag'] = {'nodes':[], 'edges': []}
    for node in dag.nodes():
        nodeobj = dag.getNode(node)
        data['dag']['nodes']+=[nodeserializer(nodeobj)]

    data['dag']['edges'] += dag.edges()

    data['rules'] = []
    for rule in rules:
        data['rules'] += [ruleserializer(rule)]

    data['applied'] = []
    for rule in applied:
        data['applied'] += [ruleserializer(rule)]

    return data

def node_to_json(nodeobj,taskserializer,proxyserializer):
    # log.info('serializing node %s %s',nodeobj.name, proxyserializer(nodeobj.resultproxy))
    nodeinfo = {
        'id':nodeobj.identifier,
        'name':nodeobj.name,
        'task':taskserializer(nodeobj.task),
        'timestamps':{
            'defined': nodeobj.define_time,
            'submit': nodeobj.submit_time,
            'ready by': nodeobj.ready_by_time
        },
        'state':str(nodeobj.state),
        'proxy':proxyserializer(nodeobj.resultproxy)
    }
    return nodeinfo

def set_generic_data(node, data):
    node.define_time = data['timestamps']['defined']
    node.submit_time = data['timestamps']['submit']
    node.ready_by_time = data['timestamps']['ready by']
    node._state = getattr(adage.nodestate,data['state'])


def dag_from_json(dagdata,nodedeserializer):
    dag = adage.graph.AdageDAG()

    for x in dagdata['nodes']:
        node = nodedeserializer(x)
        dag.addNode(node)

    for x in dagdata['edges']:
        dag.addEdge(dag.getNode(x[0]),dag.getNode(x[1]))


    return dag
