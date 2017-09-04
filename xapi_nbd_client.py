
from new_nbd_client import new_nbd_client


class xapi_nbd_client(new_nbd_client):

    def __init__(self, hostname, vdi,
                 username="<username>", password="<password>", session=None):

        if session is None:
            from xmlrpc.client import ServerProxy
            p = ServerProxy("http://" + hostname)
            session = p.session.login_with_password("<username>", "<password>")['Value']
        export_name = "nbd://{}/{}?session_id={}" .format(
                hostname, vdi, session._session)
        new_nbd_client.__init__(self, hostname, export_name)
