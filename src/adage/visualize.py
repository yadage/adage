import networkx as nx
import subprocess
import datetime

import adage.nodestate as nodestate

def node_visible(nodeobj,time):
    return time > nodeobj.define_time

def state_at_time(nodeobj,time):
    defined    = nodeobj.define_time
    submitted  = nodeobj.submit_time
    ready_by   = nodeobj.ready_by_time
    if time < defined:
        return None
    if submitted and defined <= time < submitted:
        return nodestate.DEFINED
    if ready_by and submitted <= time < ready_by:
        return nodestate.RUNNING
    if ready_by and ready_by <= time:
        return nodeobj.state
    return nodestate.DEFINED

def colorize_graph(dag,normtime = None):
    allnodes = dag.nodes()
    if allnodes:
        start = min(filter(lambda x: x,[dag.getNode(n).define_time for n in allnodes]))
        stop  = max(filter(lambda x: x,[dag.getNode(n).ready_by_time for n in allnodes]))
        time = start + normtime*(stop-start)
    else:
        time = 0
    return colorize_graph_at_time(dag,time)

def colorize_graph_at_time(dag,time):
    colorized = nx.DiGraph()
    colorkey = {
        None : None,
        nodestate.DEFINED : 'grey',
        nodestate.RUNNING : 'yellow',
        nodestate.FAILED  : 'red',
        nodestate.SUCCESS : 'green'
    }

    for node in dag.nodes():
        nodeobj = dag.getNode(node)

        state   = state_at_time(nodeobj,time)
        color   = colorkey[state]
        visible = node_visible(nodeobj,time)

        style = 'filled' if visible else 'invis'
        dot_attr = {'label':'{} '.format(nodeobj.name), 'style':style, 'color': color}

        colorized.add_node(node, **dot_attr)
        for pre in dag.predecessors(node):
            colorized.add_edge(pre,node)

    dotformat = nx.drawing.nx_pydot.to_pydot(colorized)
    dotformat.set_label(datetime.datetime.fromtimestamp(time).strftime('%Y-%m-%d %H:%M:%S'))
    for e in dotformat.get_edges():
        edge_visible =  node_visible(dag.getNode(e.get_destination().replace('"','')),time)
        if not edge_visible:
            e.set_style('invis')
    return dotformat

def save_dot(dotstring,filename,fileformat):
    with open(filename,'w') as dotoutputfile:
        p = subprocess.Popen(['dot', '-T{}'.format(fileformat), r'-Gsize=18,12\!', '-Gdpi=100'], stdout = dotoutputfile, stdin = subprocess.PIPE)
        p.communicate(dotstring.encode('ascii'))

def print_dag(dag,name,trackdir,time = None):
    pngfilename = '{}/{}.png'.format(trackdir,name)
    save_dot(colorize_graph(dag,time).to_string(),pngfilename,'png')
