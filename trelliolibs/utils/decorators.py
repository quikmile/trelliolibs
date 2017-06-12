from functools import wraps
from uuid import UUID

from cerberus import Validator
from trellio import HTTPService, TCPService

from .helpers import json_response


class TrellioValidator(Validator):
    def _validate_type_uuid(self, value):
        if isinstance(value, UUID):
            return True


def validate_schema(schema=None, allow_unknown=False):
    def decorator(func):
        @wraps(func)
        async def wrapper(self, *args, **kwargs):
            if schema:
                v = TrellioValidator(schema, allow_unknown=allow_unknown)
                if isinstance(self, HTTPService):
                    request = args[0]
                    payload = await request.json()
                    if not v.validate(payload):
                        return json_response({'error': v.errors})
                elif isinstance(self, TCPService):
                    if not v.validate(kwargs):
                        return {'error': v.errors}
            return await func(self, *args, **kwargs)

        return wrapper

    return decorator


def required_params(*params):
    def decorator(func):
        @wraps(func)
        async def wrapper(self, *args, **kwargs):
            if isinstance(self, HTTPService):
                request = args[0]
                payload = await request.json()
                missing_params = list(filter(lambda x: x not in payload.keys(), params))
                if missing_params:
                    return json_response({'error': 'required params - {} not found'.format(', '.join(missing_params))})
            elif isinstance(self, TCPService):
                missing_params = list(filter(lambda x: x not in kwargs.keys(), params))
                if missing_params:
                    return {'error': 'required params - {} not found'.format(', '.join(missing_params))}

            return await func(self, *args, **kwargs)

        return wrapper

    return decorator
