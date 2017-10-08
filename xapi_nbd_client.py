from new_nbd_client import new_nbd_client


def enable_nbd_if_necessary(session):
	has_nbd_network = False
	for network in session.xenapi.network.get_all():
		purpose = session.xenapi.network.get_purpose(network)
		if "nbd" in purpose or "insecure_nbd" in purpose:
			print("Found network on which NBD ({}) is allowed: {}".format(purpose, network))
			has_nbd_network = True
	if not has_nbd_network:
		print("WARNING: Found no network on which NBD is allowed, enabling secure NBD on ALL NETWORKS!!!!!!!")
		for network in session.xenapi.network.get_all():
			print("Enabling secure NBD on network {}".format(network))
			session.xenapi.network.add_purpose(network, "nbd")


class xapi_nbd_client(new_nbd_client):
    def __init__(self, session, vdi, use_tls=True):
        from pprint import pprint as pp

        enable_nbd_if_necessary(session)

        infos = session.xenapi.VDI.get_nbd_info(vdi)
        pp('Can connect to the following addresses:')
        pp(infos)
        info = infos[0]
        pp('Using the following:')
        pp(info)
        host = info["address"]
        export_name = info["exportname"]
        port = info["port"]
        subject = info["subject"]
        if use_tls:
            ca_cert = info["cert"]
        else:
            ca_cert = None

        new_nbd_client.__init__(
            self, host=host, export_name=export_name, port=port, ca_cert=ca_cert, subject=subject)
