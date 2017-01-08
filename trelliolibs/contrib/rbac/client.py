from trellio.services import TCPServiceClient, request


class RBACTCPClient(TCPServiceClient):
    def __init__(self, *args, **kwargs):
        super(RBACTCPClient, self).__init__("RBACTcpService", 1)

    @request
    def verify_access(self, user_id, resource, resource_action):
        return locals()  # @request requires a dict containing params describing the request payload

    @request
    def get_user_permissions(self, user_id):
        return locals()

    @request
    def create_service_resources(self, service_name, service_version, http_resources):
        return locals()
