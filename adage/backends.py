import multiprocessing
import sys

class MultiProcBackend(object):
  def __init__(self,poolsize):
    self.pool = multiprocessing.Pool(poolsize)
  def submit(self,task):
    return self.pool.apply_async(task)
  def result_of(self,result):
    return result.get()
  def ready(self,result):
    return result.ready()
  def successful(self,result):
    if not self.ready(result): return False
    return result.successful()
  def fail_info(self,result):
    try:
      self.result_of(result)
    except RuntimeError:
      t,v,tb =  sys.exc_info()
      import traceback
      traceback.print_tb(tb)
      return (t,v)
      
class CeleryBackend(object):
  def __init__(self,app):
    self.app = app
  def submit(self,task):
    self.app.set_current()
    return task.func.celery.apply_async(task.args,task.kwargs,throw = False)
  def result_of(self,result):
    return result.get()
  def ready(self,result):
    return result.ready()
  def successful(self,result):
    return result.successful()
  def fail_info(self,result):
    try:
      self.result_of(result)
    except RuntimeError:
      return sys.exc_info()
    
class DummyResult(object):    
  pass
  
class DummyBackend(object):
  def submit(self,task):
    if task:
      pass
    return DummyResult()
  def result_of(self,result):
    if result:
      pass
    return None
  def ready(self,result):
    if result:
      pass
    return True
  def successful(self,result):
    if result:
      pass
    return False
  def fail_info(self,result):
    if result:
      pass
    return 'cannot give reason :( '