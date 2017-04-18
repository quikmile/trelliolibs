from uuid import UUID
import collections
from asyncpg.exceptions import DuplicateTableError
from asyncpg.exceptions import UndefinedTableError
from trellio.wrappers import Response

try:
    import ujson as json
except:
    import json


def json_response(data, status=200):
    return Response(content_type='application/json', body=json.dumps(data).encode('utf-8'), status=status)


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
