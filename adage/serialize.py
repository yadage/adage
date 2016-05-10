import networkx as nx

def to_json(adageobj):
    dag, rules = adageobj.dag, adageobj.rules
    data = {'dag':None, 'rules':None}

    data['rules'] = {'nrules':len(rules)}
    data['dag'] = {'nodes':[]}
    for node in nx.topological_sort(dag):
        nodeobj = dag.getNode(node)
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
        data['dag']['nodes']+=[nodeinfo]
    return data