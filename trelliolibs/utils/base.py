from asyncio import Task, ensure_future, gather
from asyncio.coroutines import iscoroutinefunction, coroutine
from functools import partial, wraps
from time import time
from types import MethodType, FunctionType

from asyncpg.exceptions import UniqueViolationError, UndefinedColumnError
from trellio import request, get, put, post, delete, api
from trellio.utils.ordered_class_member import OrderedClassMembers
from trelliopg import get_db_adapter

from .decorators import TrellioValidator
from .helpers import RecordHelper, uuid_serializer, json_response, json_serializer


class RecordNotFound(Exception):
    pass


class RequestKeyError(Exception):
    pass


def view_wrapper(view):
    @wraps(view)
    async def f(self, request, *args, **kwargs):
        dispatch = getattr(self, 'dispatch', None)
        if dispatch:
            result = await self.dispatch(request, *args, **kwargs)
            if result:
                return result
        return await view(self, request, *args, **kwargs)

    return f


class WrappedViewMeta(OrderedClassMembers):
    def __new__(self, name, bases, classdict):

        for name, attr in classdict.items():
            if isinstance(attr, FunctionType) and getattr(attr, 'is_http_method', False):
                classdict[name] = view_wrapper(attr)
        return super().__new__(self, name, bases, classdict)


def extract_request_params(request, filter_keys=()):
    params = dict()

    if request.get('limit'):
        if request.get('limit').upper() == 'ALL':
            params['limit'] = 'ALL'
            request.pop('limit')
        else:
            params['limit'] = request.pop('limit')

    if request.get('offset'):
        params['offset'] = request.pop('offset')
    if request.get('order_by'):
        params['order_by'] = request.pop('order_by')

    if filter_keys:
        wrong_keys = [key for key in request.keys() if key not in filter_keys]
        if wrong_keys:
            raise RequestKeyError('{} request params not allowed. Allowed params - {}'.format(', '.join(wrong_keys),
                                                                                              ', '.join(filter_keys)))

    params['filter'] = dict()
    params['filter'].update(request)

    return params


class SignalMethodWrapper:
    """
    Wrapper object for a method to be called.
    """

    def __init__(self, instance, func, name):
        self.instance, self.func, self.name = instance, func, name
        assert instance is not None
        assert func is not None
        assert name is not None

    def __call__(self, *args, **kwargs):
        return self.instance._run_signals(self.name, self.func, *args, **kwargs)


class BaseSignal:
    registered_methods = []

    def __init__(self, signals: bool = True):
        if signals:
            self._enable_signals()

    def _enable_signals(self):
        for method in self.registered_methods:
            func = getattr(self, method)
            if type(func) is MethodType:
                wrapper = SignalMethodWrapper(self, func, method)
                setattr(self, method, wrapper)

    def _run_signals(self, name, func, *args, **kwargs):
        pre_signal = getattr(self, 'pre_{}'.format(name), None)
        post_signal = getattr(self, 'post_{}'.format(name), None)

        if pre_signal:
            self._run_coroutine(pre_signal, *args, **kwargs)

        wrapped_func = func
        if not iscoroutinefunction(func):
            wrapped_func = coroutine(func)

        future = ensure_future(wrapped_func(*args, **kwargs))
        if post_signal:
            future.add_done_callback(partial(self._run_coroutine, post_signal, *args, **kwargs))
        return future

    def _run_coroutine(self, coro, *args, **kwargs):
        print(locals())
        args = list(args)
        result = None
        if len(args) > 0 and type(args[-1]) is Task:
            task = args.pop()
            result = task.result()

        wrapper_coro = coro
        if not iscoroutinefunction(coro):
            wrapper_coro = coroutine(coro)

        ensure_future(wrapper_coro(result, *args, **kwargs))


class CRUDModel(BaseSignal):
    registered_methods = ['get', 'filter', 'create', 'update', 'delete']

    def __init__(self, table: str = '', json_fields: list = (), signals: bool = True):
        super(CRUDModel, self).__init__(signals=signals)
        self._table = table
        self._db = get_db_adapter()
        self._record = RecordHelper()
        self._serializers = [uuid_serializer, partial(json_serializer, fields=json_fields)]

    async def count(self, **filter) -> int:
        query = """select count(id) from {}""".format(self._table)
        if filter:
            query += self._db._where_query(filter, None, None, None)

        pool = await self._db.get_pool()
        async with pool.acquire() as con:
            results = await con.fetchrow(query)

        return int(results[0])

    async def get(self, **where) -> dict:
        results = await self._db.where(table=self._table, **where)
        if len(results) == 0:
            raise RecordNotFound('record does not exists')

        return self._record.record_to_dict(results[0], normalize=self._serializers)

    async def create(self, values: dict) -> dict:
        values['created'] = int(time())
        values['updated'] = values['created']
        record = await self._db.insert(table=self._table, value_dict=values)
        return self._record.record_to_dict(record, normalize=self._serializers)

    async def filter(self, limit=None, offset=None, order_by='created desc', **filter) -> list:
        record = await self._db.where(table=self._table, offset=offset, limit=limit, order_by=order_by, **filter)
        return self._record.record_to_dict(record, normalize=self._serializers)

    async def delete(self, is_active=False, **where):
        if is_active == False:
            return await self._db.update(table=self._table, where_dict=where, is_active=False)
        else:
            return await self._db.delete(table=self._table, **where)

    async def search(self, limit, columns='*', **where):
        record = await self._db.where(table=self._table, columns=columns, limit=limit, **where)
        return self._record.record_to_dict(record, normalize=self._serializers)

    async def update(self, where_dict: dict, values: dict) -> dict:
        values['updated'] = int(time())
        result = await self._db.update(table=self._table, where_dict=where_dict, **values)
        return self._record.record_to_dict(result, normalize=self._serializers)

    async def paginate(self, limit=15, offset: int = 0, order_by: str = 'created desc', **filter) -> dict:
        coros = [self.filter(limit=limit, offset=offset, order_by=order_by, **filter), self.count(**filter)]
        records, count = await gather(*coros, return_exceptions=True)
        if isinstance(records, Exception):
            raise records
        if isinstance(count, Exception):
            raise count

        if count == 0:
            return {'records': [], 'total_pages': 0, 'total_records': 0, 'limit': limit}

        if offset is None:
            offset = 0

        if limit == 'ALL' or limit is None:
            limit = count

        limit = int(limit)
        offset = int(offset)

        total_pages = (count // limit) + 1

        last_offset = limit * (total_pages - 1)
        next_offset = offset + limit

        if offset == last_offset:
            next_offset = None

        prev_offset = offset - limit
        if offset == 0:
            prev_offset = None

        return {'records': records, 'next_offset': next_offset, 'prev_offset': prev_offset, 'last_offset': last_offset,
                'total_pages': total_pages, 'total_records': count, 'limit': limit}


class CRUDTCPClient:
    @request
    def get_record(self, id):
        return locals()

    @request
    def filter_record(self, params):
        return locals()

    @request
    def create_record(self, values):
        return locals()

    @request
    def update_record(self, id, values):
        return locals()

    @request
    def delete_record(self, id):
        return locals()


class CRUDHTTPService:
    def __init__(self, name, version, host, port, table_name='', base_uri='', required_params=(), state_key=None,
                 create_schema=None, update_schema=None, allow_unknown=False, json_fields=()):
        super(CRUDHTTPService, self).__init__(name, version, host, port)
        self._table_name = table_name
        self._model = CRUDModel(table=table_name, json_fields=json_fields)
        self._state_key = state_key
        self._create_schema = create_schema
        self._update_schema = update_schema
        self._allow_unknown = allow_unknown
        self._base_uri = ''
        if base_uri:
            self._base_uri = '/{}'.format(base_uri)

    def _enable_crud(self):
        self._enable_delete()
        self._enable_filter()
        self._enable_get()
        self._enable_create()
        self._enable_update()

    def _enable_filter(self):
        path = '/all/'
        if self._base_uri:
            path = self._base_uri + 's/'
        func = get(path=path)
        self.__class__.filter_record = func(self.filter_record)

    def _enable_get(self):
        func = get(path=self._base_uri + '/{id}/')
        self.__class__.get_record = func(self.get_record)

    def _enable_create(self):
        func = post(path=self._base_uri + '/')
        self.__class__.create_record = func(self.create_record)

    def _enable_update(self):
        func = put(path=self._base_uri + '/{id}/')
        self.__class__.update_record = func(self.update_record)

    def _enable_delete(self):
        func = delete(path=self._base_uri + '/{id}/')
        self.__class__.delete_record = func(self.delete_record)

    async def filter_record(self, service, request, *args, **kwargs):
        try:
            params = extract_request_params(dict(request.GET))
            if self._state_key and request._state and request._state['user_subs'].get(self._state_key):
                params['filter'][self._state_key] = request._state['user_subs'][self._state_key]

            results = await self._model.filter(limit=params['limit'],
                                               offset=params['offset'],
                                               order_by=params['order_by'],
                                               **params['filter'])
            return json_response(results)
        except RequestKeyError as e:
            return json_response({'error': str(e)}, status=400)

    async def get_record(self, service, request, *args, **kwargs):
        id = request.match_info.get('id')
        try:
            result = await self._model.get(id=id)
            return json_response(result)
        except RecordNotFound:
            return json_response({'error': '{}_id {} does not exists'.format(self._table_name, id)}, status=400)

    async def create_record(self, service, request, *args, **kwargs):
        values = await request.json()

        if self._create_schema:
            v = TrellioValidator(self._create_schema, allow_unknown=self._allow_unknown)
            if not v.validate(values):
                return json_response({'error': v.errors}, status=400)

        if self._state_key:
            values['updated_by'] = request._state['user_id']
            values[self._state_key] = request._state['user_subs'].get(self._state_key)

        try:
            result = await self._model.create(values)
            return json_response(result)
        except UniqueViolationError:
            return json_response({'error': 'duplicate record'}, status=400)
        except UndefinedColumnError as e:
            return json_response({'error': str(e)}, status=400)

    async def update_record(self, service, request, *args, **kwargs):
        values = await request.json()

        if self._update_schema:
            v = TrellioValidator(self._update_schema, allow_unknown=self._allow_unknown)
            if not v.validate(values):
                return json_response({'error': v.errors}, status=400)

        if self._state_key:
            values['updated_by'] = request._state['user_id']
            values[self._state_key] = request._state['user_subs'].get(self._state_key)
        try:
            id = request.match_info.get('id')
            results = await self._model.update({'id': id}, values)
            if len(results) > 1:
                return json_response({'error': "multiple records found with this id '{}'".format(id)}, status=400)
            return json_response(results[0])
        except RecordNotFound as e:
            return json_response({'error': str(e)}, status=400)
        except UniqueViolationError:
            return json_response({'error': 'duplicate record'}, status=400)
        except UndefinedColumnError as e:
            return json_response({'error': str(e)}, status=400)

    async def delete_record(self, service, request, *args, **kwargs):
        return json_response(await self._model.delete(id=request.match_info.get('id')))


class CRUDTCPService:
    def __init__(self, name, version, host, port, table_name='', required_params=(), create_schema=None,
                 update_schema=None, allow_unknown=False, json_fields=()):
        super(CRUDTCPService, self).__init__(name, version, host, port)
        self._table_name = table_name
        self._model = CRUDModel(table=table_name, json_fields=json_fields)
        self._required_params = required_params
        self._create_schema = create_schema
        self._update_schema = update_schema
        self._allow_unknown = allow_unknown

    @api
    async def filter_record(self, params):
        try:
            if not params.get('filter'):
                params['filter'] = dict()
            return await self._model.filter(limit=params.get('limit'),
                                            offset=params.get('offset'),
                                            order_by=params.get('order_by'),
                                            **params.get('filter'))

        except RequestKeyError as e:
            return {'error': str(e)}

    @api
    async def get_record(self, id):
        try:
            return await self._model.get(id=id)
        except RecordNotFound:
            return {'error': '{}_id {} does not exists'.format(self._table_name, id)}

    @api
    async def create_record(self, values):
        missing_params = list(filter(lambda x: x not in values.keys(), self._required_params))
        if missing_params:
            return {'error': 'required params - {} not found'.format(', '.join(missing_params))}

        if self._create_schema:
            v = TrellioValidator(self._create_schema, allow_unknown=self._allow_unknown)
            if not v.validate(values):
                return {'error': v.errors}

        try:
            return await self._model.create(values)
        except UniqueViolationError:
            return {'error': 'duplicate record'}
        except UndefinedColumnError as e:
            return {'error': str(e)}

    @api
    async def update_record(self, id, params):
        if self._update_schema:
            v = TrellioValidator(self._update_schema, allow_unknown=self._allow_unknown)
            if not v.validate(params):
                return {'error': v.errors}

        try:
            return await self._model.update({'id': id}, params)
        except RecordNotFound as e:
            return {'error': str(e)}
        except UniqueViolationError:
            return {'error': 'duplicate record'}
        except UndefinedColumnError as e:
            return {'error': str(e)}

    @api
    async def delete_record(self, id):
        return await self._model.delete(id=id)
