import networkx as nx
import dagstate
import subprocess
import nodestate

def node_visible(node,time,start,stop):
    norm_node_time = (node.submit_time-start)/(stop-start)
    return norm_node_time < time    

def colorize_graph(dag,normtime = None):
    colorized = nx.DiGraph()
    allnodes = dag.nodes()
    starttimes = [dag.getNode(n).define_time for n in allnodes]
    stoptimes  = [dag.getNode(n).ready_by_time for n in allnodes]
    start,stop = min(starttimes),max(stoptimes)

    # print "total delta: {}".format(stop-start)
    for node in dag.nodes():
        nodeobj = dag.getNode(node)
        
        time = start + normtime*(stop-start)

        # joblength = nodeobj.ready_by_time-nodeobj.submit_time
        # print 'joblength: {} {}'.format(nodeobj.name,joblength)

        color = None
        if start <= time < nodeobj.submit_time:
            color = 'blue' if dagstate.upstream_failure(dag,nodeobj) else 'grey'
        if nodeobj.submit_time <= time < nodeobj.ready_by_time:
            color = 'yellow'
        if nodeobj.ready_by_time <= time <= stop:
            if nodeobj.state()==nodestate.FAILED:
                color = 'red'
            if nodeobj.state()==nodestate.SUCCESS:
                color = 'green'

        visible = node_visible(nodeobj,normtime,start,stop)
        # hmtimes = [start,nodeobj.submit_time,nodeobj.ready_by_time,stop]
        # hmnormes = [(t-start)/(stop-start) for t in hmtimes]
        # print 'times: {} {} {} {}'.format(normtime,hmnormes,color,visible)

        style = 'filled' if visible else 'invis'
        dot_attr = {'label':'{} '.format(nodeobj.name), 'style':style, 'color': color}

        colorized.add_node(node,dot_attr)
        for pre in dag.predecessors(node):
            colorized.add_edge(pre,node)
    
    # print '------------'
    dotformat = nx.drawing.nx_pydot.to_pydot(colorized)
    for e in dotformat.get_edges():
        edge_visible =  node_visible(dag.getNode(e.get_destination().replace('"','')),normtime,start,stop)
        if not edge_visible:
            e.set_style('invis')
    return dotformat

def print_dag(dag,name,trackdir,time = None):
    dotfilename = '{}/{}.dot'.format(trackdir,name)
    pngfilename = '{}/{}.png'.format(trackdir,name) 

    open(dotfilename,'w').write(colorize_graph(dag,time).to_string())
    with open(pngfilename,'w') as pngfile:
        subprocess.call(['dot','-Tpng','-Gsize=18,12\!','-Gdpi=100 ',dotfilename], stdout = pngfile)
        subprocess.call(['convert',pngfilename,'-gravity','North','-background','white','-extent','1800x1200',pngfilename])