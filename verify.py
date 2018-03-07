"""Helper for verifying certificates"""

import requests
from requests.adapters import HTTPAdapter


class CustomHostnameCheckingAdapter(HTTPAdapter):
    """Verifies that the certificate matches the specified hostname"""

    def __init__(self, hostname):
        super().__init__()
        self._hostname = hostname

    def cert_verify(self, conn, url, verify, cert):
        conn.assert_hostname = self._hostname
        return super().cert_verify(conn, url, verify, cert)

def session_for_host(session, host):
    """
    Returns a requests session suitable for connecting to the given host.
    The session will expect the server name to be the hostname for https connections.
    """
    hostname = session.xenapi.host.get_hostname(host)
    s = requests.Session()
    s.mount('https://', CustomHostnameCheckingAdapter(hostname))
    return s
