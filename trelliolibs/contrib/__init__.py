def get_current_rbac_service_details():
	from collections import namedtuple
	service_details = namedtuple('service_details', ['HTTP', 'TCP'])
	_sd = service_details()
	_sd.HTTP = ('RBACHTTPService', 1)
	_sd.TCP = ('RBACTcpService', 1)
	return _sd