from .client import RBACTCPClient
from .exceptions import AccessError


class VerifyUserAccess:
    middleware_info = 'RBAC access control'
    rbac_client = None  # trellio needs a self configurable tcpclient

    def pre_request(self, service, request, *args, **kwargs):
        if not self.rbac_client:
            for i in dir(service):
                if isinstance(i, RBACTCPClient):
                    self.rbac_client = i
                    break
        if not self.rbac_client:
            raise Exception('Middleware need a rbac client')
        user_id = request.headers['USER_JWT']['user_id']
        resource = request.rel_url  # aiopg request attr
        resource_action = request.method
        access = self.rbac_client.verify_access(user_id, resource, resource_action)
        if not access['access']:
            raise AccessError
