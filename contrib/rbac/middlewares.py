from contrib.rbac.client import RBACTCPClient
from contrib.rbac.exceptions import AccessError

class VerifyUserAccess:
    middleware_info = 'RBAC access control'

    def __init__(self):
        self.rbac_client = RBACTCPClient()

    def pre_request(self, request, *args, **kwargs):
        user_id = request.headers['USER_JWT']['user_id']
        resource = request.rel_url#aiopg request attr
        resource_action = request.method
        if not self.rbac_client.verify_access(user_id,resource,resource_action):
            raise AccessError
