import json
import logging
import inspect
from trellio.host import Host
from trellio.signals import register, ServiceReady


def activate_resources(rbac_service_name='rbac_service', rbac_service_version=1):
    from trelliolibs.contrib.rbac.client import RBACTCPClient
    rbac_client = RBACTCPClient()

    @register(ServiceReady)
    async def register_service_resources():
        nonlocal rbac_client
        _logger = logging.getLogger(__name__)
        http_service = Host._http_service
        service_name = http_service.name
        service_version = http_service.version
        api_methods = []
        exclude_resources = ['/ping', '/_stats', '/ping/', '/_stats/']
        for i in dir(http_service):
            attr = getattr(http_service, i)
            if inspect.ismethod(attr) and getattr(attr, 'is_http_method', None):
                api_methods.append(attr)
        resource_dict = {}
        for i in api_methods:
            path = i.paths[0]
            method = i.method
            if path not in exclude_resources:
                if resource_dict.get(path):
                    resource_dict[path].append(method)
                else:
                    resource_dict[path] = [method]
        result = await rbac_client.create_service_resources(service_name, service_version, http_resources=resource_dict)
        _logger.info('RBAC auto-create service resources response %s' % str(json.dumps(result)))