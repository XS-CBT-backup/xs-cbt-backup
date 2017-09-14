from new_nbd_client import new_nbd_client


class xapi_nbd_client(new_nbd_client):
    def __init__(self,
                 hostname,
                 vdi,
                 username="root",
                 password="xenroot",
                 session=None):

        if session is None:
            import XenAPI
            import os
            session = XenAPI.Session("http://" + hostname)
            username = username or os.environ['XS_USERNAME']
            password = password or os.environ['XS_PASSWORD']
            session.xenapi.login_with_password(username, password)
        export_name = "nbd://{}/{}?session_id={}".format(
            hostname, vdi, session._session)
        new_nbd_client.__init__(self, hostname, export_name)
