import functools

def qualifiedname(thing):
  if thing.__module__ != '__main__':
    return '{}.{}'.format(thing.__module__,thing.__name__)
  else:
    return thing.__name__

class DelayedCallable(object):
  def __init__(self,func):
    self.qualname = qualifiedname(func)
    self.func = func
    self.args = None
    self.kwargs = None
    
  def __repr__(self):
    return '<Delayed: {}>'.format(self.qualname)
    
  def __call__(self):
    assert self.kwargs is not None
    assert self.args is not None
    return self.func(*self.args,**self.kwargs)

  def setargs(self,*args,**kwargs):
    self.args = args
    self.kwargs = kwargs
    return self

class RuleFunctor(object):
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

def rulefunc(func):
  def sig(*args,**kwargs):
    instance = RuleFunctor(func)
    instance.setargs(*args,**kwargs)
    return instance
  func.s = sig
  return func

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


