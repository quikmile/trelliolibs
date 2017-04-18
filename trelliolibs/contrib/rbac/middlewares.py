from asyncio.tasks import wait_for

try:
    import ujson as json
except:
    import json

from aiohttp.web import Response

from .client import RBACTCPClient


class VerifyUserAccess:
    middleware_info = 'VerifyUserAccessMiddleware'
    rbac_client = RBACTCPClient()

    async def pre_request(self, service, request, *args, **kwargs):
        if not self.rbac_client:
            raise Exception('Middleware need a rbac client')
        auth_token = request.headers['AUTHORIZATION']
        resource = str(request.rel_url)  # aiohttp request attr
        resource_action = request.method.lower()
        access = await wait_for(self.rbac_client.verify_access(auth_token=auth_token, resource_name=resource,
                                                               resource_action=resource_action,
                                                               resource_type='http'), timeout=None)
        if not access['access']:
            return Response(status=400, content_type='application/json',
                            body=json.dumps(
                                access).encode())


class RequestUser:
    middleware_info = 'RequestUserMiddleware'

    async def pre_request(self, service, request, *args, **kwargs):
        user_id = request.headers.get('x-user-id', None)
        user_subs = request.headers.get('x-user-subs', None)

        if not user_id and not user_subs:
            rbac_client = RBACTCPClient()
            response = await wait_for(rbac_client.verify_token(request.headers.get('authorization')))
            user_id = response.get('user_id')
            user_subs = response.get('user_subs')

        request.__setitem__('user_id', user_id)
        request.__setitem__('user_subs', json.loads(user_subs))
