import networkx as nx
import json
import adage.adageobject

class DefaultAdageEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, adage.adageobject):
            return obj_to_json(obj)
        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, obj)

def obj_to_json(adageobj):
    dag, rules, applied = adageobj.dag, adageobj.rules, adageobj.applied_rules
    data = {'dag':None, 'rules':None, 'applied rules':None}
    
    data['dag'] = {'nodes':[]}
    for node in nx.topological_sort(dag):
        nodeobj = dag.getNode(node)
        data['dag']['nodes']+=[node_to_json(dag,nodeobj)]


    data['rules'] = []
    for rule in rules:
        data['rules'] += [rule_to_json(rule)]

    data['applied'] = []
    for rule in applied:
        data['applied'] += [rule_to_json(rule)]
        
    return data

def node_to_json(dag,nodeobj):
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
    return nodeinfo
    
def rule_to_json(rule):
    ruleinfo = {
        'type':str(type(rule))
    }
    return ruleinfo