def qualifiedname(thing):
    if thing.__module__ != '__main__':
        return '{}.{}'.format(thing.__module__,thing.__name__)
    else:
        return thing.__name__


class Functor(object):
    def __init__(self,func):
        self.qualname = qualifiedname(func)
        self.func = func
        self.args = None
        self.kwargs = None
    
    def __call__(self,dag):
        updated = self.kwargs.copy()
        updated.update(dag = dag)
        return self.func(*self.args,**updated)

    def setargs(self,*args,**kwargs):
        self.args = args
        self.kwargs = kwargs
        return self

def functorize(func):
    def sig(*args,**kwargs):
        instance = Functor(func)
        instance.setargs(*args,**kwargs)
        return instance
    func.s = sig
    return func
    
class Rule(object):
    def __init__(self,predicate,body):
        self.predicate = predicate
        self.body = body

    def applicable(self,dag):
        return self.predicate(dag)

    def apply(self,dag):
        return self.body(dag)
    
class DelayedCallable(object):
    def __init__(self,func):
        self.qualname = qualifiedname(func)
        self.func = func
        self.args = None
        self.kwargs = None
        
    def __repr__(self):
        return '<Delayed: {}>'.format(self.func)
        
    def __call__(self):
        assert self.kwargs is not None
        assert self.args is not None
        return self.func(*self.args,**self.kwargs)

    def setargs(self,*args,**kwargs):
        self.args = args
        self.kwargs = kwargs
        return self

def adagetask(func):
    try:
        from celery import shared_task
        func.celery = shared_task(func)
    except ImportError:
        pass        

    def sig(*args,**kwargs):
        instance = DelayedCallable(func)
        instance.setargs(*args,**kwargs)
        return instance
    func.s = sig
    return func


