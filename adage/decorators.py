import functools

def adageop(func):
    """
    Decorator that adds a 's' attribute to a function
    The attribute can be used to partially define
    the function call, except for the 'adageobj'
    keyword argument, the return value is a
    single-argument ('adageobj') function
    """
    def partial(*args,**kwargs):
        return functools.partial(func,*args,**kwargs)
    func.s = partial
    return func
    
class Rule(object):
    def __init__(self,predicate,body):
        self.predicate = predicate
        self.body = body

    def applicable(self,adageobj):
        return self.predicate(adageobj)

    def apply(self,adageobj):
        return self.body(adageobj)

def adagetask(func):
    """
    Decorator that adds a 's' attribute to a function
    The attribute can be used to fully define a function
    call to be executed at a later time. The result
    will be a zero-argument callable
    """
    try:
        from celery import shared_task
        func.celery = shared_task(func)
    except ImportError:
        pass        

    def partial(*args,**kwargs):
        return functools.partial(func,*args,**kwargs)
    func.s = partial
    return func
    
def callbackrule(after = None):
    """
    A decorator that creates a adage Rule from a callback function
    The after argument is expected to container a dictionary
    of node identifiers. The callback is expected have two arguments
    A dictionary with the same keys as in after as keys, and the 
    corresponding nodes as values, as well as the adajeobj will be
    passed to the callback
    """
    after = after or {}
    def decorator(func):
        def predicate(adageobj):
            return all([adageobj.dag.getNode(node).successful() for node in after.values()])
        def body(adageobj):
            depnodes = {k:adageobj.dag.getNode(v) for k,v in after.iteritems()}
            func(depnodes = depnodes, adageobj = adageobj)
            
        return Rule(predicate,body)
    return decorator
