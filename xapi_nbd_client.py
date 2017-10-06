from new_nbd_client import new_nbd_client


def get_all_certificates(session):
    cacert = ""
    for host in session.xenapi.host.get_all():
        cacert += session.xenapi.host.get_server_certificate(host) + "\n"
    return cacert


class xapi_nbd_client(new_nbd_client):
    def __init__(self, session, vdi, use_tls=True):
        from pprint import pprint as pp

        infos = session.xenapi.VDI.get_nbd_info(vdi)
        pp('Can connect to the following addresses:')
        pp(infos)
        info = infos[0]
        host = info["address"]
        export_name = info["exportname"]
        port = info["port"]

        if use_tls:
            ca_cert = get_all_certificates(session)
        else:
            ca_cert = None

        new_nbd_client.__init__(
            self, host=host, export_name=export_name, port=port, ca_cert=ca_cert)
