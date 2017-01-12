from functools import wraps

from trellio import HTTPService, TCPService

from .helpers import json_response


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
                return {'error': 'required params - {} not found'.format(', '.join(missing_params))}

            return await func(self, *args, **kwargs)

        return wrapper

    return decorator
