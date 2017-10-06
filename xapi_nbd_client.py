from new_nbd_client import new_nbd_client


class xapi_nbd_client(new_nbd_client):
    def __init__(self, session, vdi, use_tls=True):
        from pprint import pprint as pp

        infos = session.xenapi.VDI.get_nbd_info(vdi)
        pp('Can connect to the following addresses:')
        pp(infos)
        info = infos[0]
        pp('Using the following:')
        pp(info)
        host = info["address"]
        export_name = info["exportname"]
        port = info["port"]
        if use_tls:
            ca_cert = info["cert"]
        else:
            ca_cert = None

        new_nbd_client.__init__(
            self, host=host, export_name=export_name, port=port, ca_cert=ca_cert)
