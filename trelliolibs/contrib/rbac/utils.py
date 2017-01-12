from trelliolibs.contrib.rbac import get_current_rbac_service_details

class RbacTcpClientMissing(Exception):
	pass

async def register_service_resources(host_class):
	tcp_clients = host_class.get_tcp_clients()
	service_details = get_current_rbac_service_details()
	rbac_client = None
	for i in tcp_clients:
		if (i.name,i.version) = service_details.TCP:
			rbac_client = i
	if not rbac_client:
		raise RbacTcpClientMissing()

	http_service = host_class.get_http_service()
	resource_kwargs = {'service_name':http_service.name, 'service_version':http_service.version, 'http_resources':[]}
	for i in dir(http_service):
		_res = getattr(http_service, i, None)
		if _res:
			if getattr(_res, 'is_http_method', None):
				_res = _res.paths #trellio version dependendant code
				resource_kwargs['http_resources'].extend(_res)

	await rbac_client.create_service_resources(**resource_kwargs)