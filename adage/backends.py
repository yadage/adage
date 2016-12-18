import multiprocessing
import sys
import traceback

class MultiProcBackend(object):
    def __init__(self,poolsize):
        self.pool = multiprocessing.Pool(poolsize)

    def submit(self,task):
        return self.pool.apply_async(task)

    def result(self,resultproxy):
        return resultproxy.get()

    def ready(self,resultproxy):
        return resultproxy.ready()

    def successful(self,resultproxy):
        if not self.ready(resultproxy): return False
        return resultproxy.successful()

    def fail_info(self,resultproxy):
        try:
            self.result(resultproxy)
        except:
            t,v,tb =    sys.exc_info()
            traceback.print_tb(tb)
            return (t,v)

class CeleryBackend(object):
    def __init__(self,app):
        self.app = app
    def submit(self,task):
        self.app.set_current()
        return task.func.celery.apply_async(task.args,task.kwargs,throw = False)

    def result(self,resultproxy):
        return resultproxy.get()

    def ready(self,resultproxy):
        return resultproxy.ready()

    def successful(self,resultproxy):
        return resultproxy.successful()

    def fail_info(self,resultproxy):
        try:
            self.result(resultproxy)
        except:
            return sys.exc_info()


class IPythonParallelBackend(object):
    def __init__(self,client, resolve_like_partial = False):
        self.client = client
        self.resolve = resolve_like_partial
        self.view = self.client.load_balanced_view()

    def submit(self,task):
        if self.resolve:
            return self.view.apply(task.func,*task.args,**task.keywords)
        return self.view.apply(task)

    def result(self,resultproxy):
        return resultproxy.get()

    def ready(self,resultproxy):
        return resultproxy.ready()

    def successful(self,resultproxy):
        return resultproxy.successful()

    def fail_info(self,resultproxy):
        return resultproxy.exception_info()

class DummyResultProxy(object):
    pass

class DummyBackend(object):
    def submit(self,task):
        if task:
            pass
        return DummyResultProxy()

    def result(self,resultproxy):
        return None

    def ready(self,resultproxy):
        return True

    def successful(self,resultproxy):
        return False

    def fail_info(self,resultproxy):
        return 'cannot give reason :( '
