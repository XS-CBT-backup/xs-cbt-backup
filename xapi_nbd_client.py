from new_nbd_client import new_nbd_client


def get_all_certificates(session):
    cacert = ""
    for host in session.xenapi.host.get_all():
        cacert += session.xenapi.host.get_server_certificate(host) + "\n"
    return cacert


class xapi_nbd_client(new_nbd_client):
    def __init__(self, session, vdi, use_tls=True):
        from urllib.parse import urlparse
        from pprint import pprint as pp

        uris = session.xenapi.VDI.get_nbd_info(vdi)
        pp('Can connect to the following URIs:')
        pp(uris)
        uri = uris[0]
        pp('Connecting to URI:')
        pp(uri)
        uri = urlparse(uri)
        host_and_port = uri.netloc.split(':')
        host = host_and_port[0]
        try:
            port = host_and_port[1]
        except:
            port = None
        export_name = uri.path + '?' + uri.query

        if use_tls:
            ca_cert = get_all_certificates(session)
        else:
            ca_cert = None

        new_nbd_client.__init__(
            self, host=host, export_name=export_name, port=port, ca_cert=ca_cert)
