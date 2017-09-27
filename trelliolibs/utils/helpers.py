import collections
from uuid import UUID

from asyncpg.exceptions import DuplicateTableError
from asyncpg.exceptions import UndefinedTableError
from trellio.wrappers import Response, Request

try:
    import ujson as json
except:
    import json


def json_response(data, status=200):
    return Response(content_type='application/json', body=json.dumps(data).encode('utf-8'), status=status)


def paginated_json_response(request: Request, records: list = (), limit=10, prev_offset=None, next_offset=None,
                            last_offset=None, total_records=None, total_pages=None) -> Response:
    if request.headers.get('X-Original-URI'):
        path = request.headers['X-Original-URI'].split('?')[0]
        base_url = '{}://{}/{}/'.format(request.scheme, request.host.strip('/'), path.strip('/'))
    else:
        base_url = request.path

    url = '{}?limit={}'.format(base_url, limit)

    links = []

    first_url = url + '&offset={}'.format(0)
    links.append('<{}>; rel="{}"'.format(first_url, 'first'))

    if next_offset:
        next_url = url + '&offset={}'.format(next_offset)
        links.append('<{}>; rel="{}"'.format(next_url, 'next'))

    if prev_offset:
        prev_url = url + '&offset={}'.format(prev_offset)
        links.append('<{}>; rel="{}"'.format(prev_url, 'prev'))

    if last_offset:
        last_url = url + '&offset={}'.format(last_offset)
        links.append('<{}>; rel="{}"'.format(last_url, 'last'))

    if total_records:
        links.append('<{}>; rel="{}"'.format(total_records, 'total_records'))

    if total_pages:
        links.append('<{}>; rel="{}"'.format(total_pages, 'total_pages'))

    headers = {'Link': ', '.join(links)} if links else {}
    headers['Access-Control-Expose-Headers'] = 'Link'
    return Response(content_type='application/json', body=json.dumps(records).encode('utf-8'), status=200,
                    headers=headers)


def json_file_to_dict(_file: str) -> dict:
    """
    convert json file data to dict

    :param str _file: file location including name

    :rtype: dict
    :return: converted json to dict
    """
    config = None
    with open(_file) as config_file:
        config = json.load(config_file)

    return config


def uuid_serializer(data):
    for key, value in data.items():
        if isinstance(value, UUID):
            data[key] = str(value)
        if value is None:
            data[key] = ''
    return data


def json_serializer(data, fields=()):
    for field in fields:
        if data.get(field):
            data[field] = json.loads(data[field])
    return data


class RecordHelper:
    @staticmethod
    def record_to_dict(recs, normalize=None):
        if normalize and not isinstance(normalize, collections.Iterable):
            normalize = [normalize]
        elif not normalize:
            normalize = normalize if normalize else [lambda i: i]

        if not isinstance(recs, list):
            data = dict(recs)
            for j in normalize:
                data = j(data)
            return data
        elif isinstance(recs, list):
            _l = []
            for i in recs:
                data = dict(i)
                for j in normalize:
                    data = j(data)
                _l.append(data)
            return _l

    @staticmethod
    def record_to_tuple(recs, normalize=None):
        if normalize and not isinstance(normalize, collections.Iterable):
            normalize = [normalize]
        elif not normalize:
            normalize = normalize if normalize else [lambda i: i]
        _l = []
        if not isinstance(recs, list):
            recs = [recs]
        for i in recs:
            data = tuple(i)
            for j in normalize:
                data = j(data)
            _l.append(data)
        if len(_l) > 1:
            return _l
        elif len(_l) > 0:
            return _l[0]
        else:
            return _l


def run_coro(coro):
    import asyncio
    return asyncio.get_event_loop().run_until_complete(coro)


async def create_table(conn, query):
    if 'CREATE' in query:
        try:
            await conn.execute(query)
            return True
        except DuplicateTableError:
            return False
    else:
        return False


async def drop_table(conn, query):
    if 'DROP' in query:
        try:
            await conn.execute(query)
            return True
        except UndefinedTableError:
            return False
    else:
        return False


async def type_cast_payload(schema: dict, payload: dict):
    """
    Make schema little forgiving! use case:  multipart data
    """
    # In ideal case this should be done using serializer. A temp measure right now
    cerberus_type_map = {
        'string': str,
        'integer': int,
        'float': float,
    }
    for field, validator in schema.items():
        field_type = validator['type']
        if field_type in ['integer', 'string', 'float']:
            payload_value = payload.get(field)
            if not payload_value:
                break
            try:
                payload[field] = cerberus_type_map[field_type](payload_value)
            except ValueError:
                break
    return payload
