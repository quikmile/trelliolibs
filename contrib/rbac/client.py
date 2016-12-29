from trellio.services import TCPServiceClient,request,subscribe
import asyncio

class RBACTCPClient(TCPServiceClient):
    def __init__(self, *args, **kwargs):
        super(RBACTCPClient, self).__init__("RBACTcpService",1)

    @request
    def verify_access(self, user_id, resource, resource_action):
        return locals() #@request requires a dict containing params describing the request payload

    @subscribe
    def user_permissions_updated(self, *args, **kwargs):
        pass