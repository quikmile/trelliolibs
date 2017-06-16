from time import time

from asyncpg.exceptions import UniqueViolationError, UndefinedColumnError
from trellio import request, get, put, post, delete, api
from trelliopg import get_db_adapter

from .decorators import TrellioValidator
from .helpers import RecordHelper, uuid_serializer, json_response


class RecordNotFound(Exception):
    pass


class RequestKeyError(Exception):
    pass


def extract_request_params(request, filter_keys=()):
    params = dict()
    if request.get('limit') is not None and request.get('limit') == 'ALL':
        params['limit'] = 'ALL'
        request.pop('limit')
    else:
        params['limit'] = int(request.pop('limit', 25))
    params['offset'] = int(request.pop('offset', 0))
    params['order_by'] = request.pop('order_by', 'created desc')

    if filter_keys:
        wrong_keys = [key for key in request.keys() if key not in filter_keys]
        if wrong_keys:
            raise RequestKeyError('{} request params not allowed. Allowed params - {}'.format(', '.join(wrong_keys),
                                                                                              ', '.join(filter_keys)))

    params['filter'] = dict()
    params['filter'].update(request)

    return params


class CRUDModel:
    def __init__(self, table=''):
        self._table = table
        self._db = get_db_adapter()
        self._record = RecordHelper()
        self._serializers = [uuid_serializer]

    async def get(self, id=None):
        results = await self._db.where(table=self._table, id=id)
        if len(results) == 0:
            raise RecordNotFound('fleet_id "{}" does not exists'.format(id))

        return self._record.record_to_dict(results[0], normalize=self._serializers)

    async def create(self, values):
        values['created'] = int(time())
        values['updated'] = values['created']
        record = await self._db.insert(table=self._table, value_dict=values)
        return self._record.record_to_dict(record, normalize=self._serializers)

    async def filter(self, limit=25, offset=0, order_by='created desc', **filter):
        record = await self._db.where(table=self._table, offset=offset, limit=limit, order_by=order_by, **filter)
        return self._record.record_to_dict(record, normalize=self._serializers)

    async def delete(self, id):
        await self._db.delete(table=self._table, where_dict={'id': id})

    async def update(self, id, values):
        values['updated'] = int(time())
        result = await self._db.update(table=self._table, where_dict={'id': id}, **values)
        return self._record.record_to_dict(result, normalize=self._serializers)


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
                 create_schema=None, update_schema=None, allow_unknown=False):
        super(CRUDHTTPService, self).__init__(name, version, host, port)
        self._table_name = table_name
        self._model = CRUDModel(table=table_name)
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
            if self._state_key:
                params[self._state_key] = request._state.get(self._state_key)

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
            return json_response(await self._model.update(request.match_info.get('id'), values))
        except RecordNotFound as e:
            return json_response({'error': str(e)}, status=400)
        except UniqueViolationError:
            return json_response({'error': 'duplicate record'}, status=400)
        except UndefinedColumnError as e:
            return json_response({'error': str(e)}, status=400)

    async def delete_record(self, service, request, *args, **kwargs):
        return json_response(await self._model.delete(request.match_info.get('id')))


class CRUDTCPService:
    def __init__(self, name, version, host, port, table_name='', required_params=(), create_schema=None,
                 update_schema=None, allow_unknown=False):
        super(CRUDTCPService, self).__init__(name, version, host, port)
        self._table_name = table_name
        self._model = CRUDModel(table=table_name)
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
            return await self._model.update(id, params)
        except RecordNotFound as e:
            return {'error': str(e)}
        except UniqueViolationError:
            return {'error': 'duplicate record'}
        except UndefinedColumnError as e:
            return {'error': str(e)}

    @api
    async def delete_record(self, id):
        return await self._model.delete(id)
