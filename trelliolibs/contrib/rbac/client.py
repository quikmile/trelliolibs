from trellio import TCPServiceClient, request


class RBACTCPClient(TCPServiceClient):
    def __init__(self, *args, **kwargs):
        super(RBACTCPClient, self).__init__("rbac_service", '1')

    @request
    def bulk_update_and_reassign_collection_roles(self, service_name, service_version, http_resources):
        return locals()

    @request
    def create_resource_action(self, resource_id, action_name, action_value):
        return locals()

    @request
    def create_service_resources(self, service_name, service_version, http_resources):
        return locals()

    @request
    def verify_access(self, resource_name, resource_action, resource_type, user_id='', auth_token=''):
        return locals()

    @request
    def verify_token(self, token):
        return locals()
