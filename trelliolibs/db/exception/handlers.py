from asyncpg import exceptions as database_exc
import asyncio


class ExceptionHandler:

    def __init__(self, conn, ds):
        self.conn = conn
        self.ds = ds

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        handler = getattr(self, 'handle_%s' % exc_type.__name__)
        if not handler:
            raise exc_val
        else:
            self.ds['exception_dict'] = handler(exc_type, exc_val, exc_tb)


class AsyncpgExceptionHandler:
    '''
    Usage with AsyncpgExceptionHandler(conn, coro_to_run_on_exception, result_dict):
            any query_code
    Or
    class Myhandler(ExceptionHandler):
        def handle_<exception_name>:
            #exception_handling code
    '''

    def __init__(self, conn, coro, ds):
        self.conn = conn
        self.coro = coro
        self.ds = ds


    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        found = False
        for i in dir(database_exc):
            excp = getattr(database_exc, i)
            if excp is exc_type:
                self.ds['exception_dict'] = asyncio.get_event_loop().run_until_complete(self.coro(exc_type, exc_val, exc_tb))
                found = True
        if not found:
            raise exc_val

class RecordNotFound(Exception):
    pass

