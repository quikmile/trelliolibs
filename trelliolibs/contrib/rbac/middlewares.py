import asyncio
import json
from trelliolibs.contrib.rbac.client import RBACTCPClient
from aiohttp.web import Response

class VerifyUserAccess:
    middleware_info = 'RBAC access control'
    rbac_client = RBACTCPClient()


    async def pre_request(self, service, request, *args, **kwargs):
        if not self.rbac_client:
            raise Exception('Middleware need a rbac client')
        auth_token = request.headers['AUTHORIZATION']
        resource = str(request.rel_url)  # aiohttp request attr
        resource_action = request.method.lower()
        access =  await asyncio.wait_for(self.rbac_client.verify_access(auth_token=auth_token, resource_name=resource,
                                                 resource_action=resource_action, resource_type='http'), timeout=None)
        if not access['access']:
            return Response(status=400, content_type='application/json',
                     body=json.dumps(
                         access).encode())