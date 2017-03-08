import asyncio
from collections import OrderedDict
from concurrent.futures.thread import ThreadPoolExecutor

_async_no_lo = None
_no_lo = None

class LRU(OrderedDict):
    '''
    DS to maintain limited number of items in dict, but in order of insert
    '''
    def __init__(self, *args, **kwds):
        self.size_limit = kwds.pop("size_limit", None)
        OrderedDict.__init__(self, *args, **kwds)
        self._check_size_limit()

    def __setitem__(self, key, value, *args, **kwargs):
        OrderedDict.__setitem__(self, key, value, *args, **kwargs)
        self._check_size_limit()

    def _check_size_limit(self):
        '''
        :return:
        '''
        if self.size_limit is not None:
          while len(self) > self.size_limit:
            self.popitem(last=False)#remove firs


class NotificationWorker:
    '''
    Notification worker agnostic of type of notification i.e android push, email, sms etc, it just
    accepts the handler+data and appends it in process queue
    '''

    def __init__(self):
        self.current_id = 1
        self.request_pool = LRU(size_limit=10000)#TODO remove hardcoded
        self.pool = ThreadPoolExecutor(max_workers=1000)#TODO remove hardcoded


    def get_notification_status(self, temp_id):
        return self.request_pool.get(temp_id)

    def submit(self, method, method_args=(), method_kwargs={}, done_callback=None, done_kwargs={}, loop=None):
        '''
        used to send async notifications
        :param method:
        :param method_args:
        :param method_kwargs:
        :param done_callback:
        :param done_kwargs:
        :param loop:
        :return:
        '''
        _future = self.pool.submit(method, *method_args, **method_kwargs)
        self.current_id += 1
        if done_callback:
            _future.add_done_callback(lambda _f:done_callback(_f,loop,**done_kwargs))#done kwargs, hardcoded kwargs
        self.request_pool[self.current_id] = _future
        return self.current_id, _future#both are volatile


def get_notification_worker():
    global _no_lo
    if _no_lo:
        return _no_lo
    else:
        _no_lo = NotificationWorker()
        return _no_lo
