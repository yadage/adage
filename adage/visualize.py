import glob
import networkx as nx
import dagstate
import subprocess
import os
import nodestate

def print_next_dag(dag,trackdir):
    nextnr = 0
    if glob.glob('{}/*.dot'.format(trackdir)):
        nextnr = max([int(os.path.basename(s).replace('dag','').replace('.dot','')) for s in    glob.glob('{}/*.dot'.format(trackdir))])+1
    
    padded = '{}'.format(nextnr).zfill(3)
    print_dag(dag,'dag{}'.format(padded),trackdir)
    
def colorize_graph(dag):
    colorized = nx.DiGraph()
    for node in dag.nodes():
        nodeobj = dag.getNode(node)

        color_dict = {
            nodestate.DEFINED: 'grey',
            nodestate.RUNNING: 'yellow',
            nodestate.FAILED: 'red',
            nodestate.SUCCESS: 'green'
        }
        
        dot_attr = {'label':'{} '.format(nodeobj.name), 'style':'filled', 'color': color_dict[nodeobj.state()]}

        #for nodes that have an upstream failure, let's do a special color (their state will be DEFINED)
        if nodeobj.state() == nodestate.DEFINED and dagstate.upstream_failure(dag,nodeobj):
            dot_attr['color'] = 'blue'


        colorized.add_node(node,dot_attr)
        for pre in dag.predecessors(node):
            colorized.add_edge(pre,node)
        
    return colorized

def print_dag(dag,name,trackdir):
    dotfilename = '{}/{}.dot'.format(trackdir,name)
    pngfilename = '{}/{}.png'.format(trackdir,name) 
    colorized = colorize_graph(dag)

    nx.write_dot(colorized,dotfilename)
    with open(pngfilename,'w') as pngfile:
        subprocess.call(['dot','-Tpng','-Gsize=18,12\!','-Gdpi=100 ',dotfilename], stdout = pngfile)
        subprocess.call(['convert',pngfilename,'-gravity','North','-background','white','-extent','1800x1200',pngfilename])
