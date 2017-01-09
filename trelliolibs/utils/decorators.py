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
                diff = set(params).symmetric_difference(payload.keys())
                if diff:
                    return json_response({'error': 'required params - {} not found'.format(', '.join(diff))})
            elif isinstance(self, TCPService):
                diff = set(params).symmetric_difference(kwargs.keys())
                return {'error': 'required params - {} not found'.format(', '.join(diff))}

            return await func(self, *args, **kwargs)

        return wrapper

    return decorator
