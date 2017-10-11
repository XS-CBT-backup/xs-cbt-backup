from new_nbd_client import new_nbd_client
import cbt_tests


class xapi_nbd_client(new_nbd_client):
    def __init__(self,
                 session,
                 vdi,
                 use_tls=True,
                 auto_enable_nbd=True,
                 skip_vlan_networks=True,
                 vdi_nbd_server_info=None):
        from pprint import pprint as pp

        self._flushed = True
        self._closed = True

        if auto_enable_nbd:
            cbt_tests.enable_nbd_if_necessary(
                session=session,
                use_tls=use_tls,
                skip_vlan_networks=skip_vlan_networks)

        if vdi_nbd_server_info is None:
            infos = session.xenapi.VDI.get_nbd_info(vdi)
            pp('Can connect to the following addresses:')
            pp(infos)
            vdi_nbd_server_info = infos[0]
            pp('Using the following:')
            pp(vdi_nbd_server_info)

        host = vdi_nbd_server_info["address"]
        export_name = vdi_nbd_server_info["exportname"]
        port = vdi_nbd_server_info["port"]
        subject = vdi_nbd_server_info["subject"]
        if use_tls:
            ca_cert = vdi_nbd_server_info["cert"]
        else:
            ca_cert = None

        new_nbd_client.__init__(
            self,
            host=host,
            export_name=export_name,
            port=port,
            ca_cert=ca_cert,
            subject=subject)
