"""Helper for verifying certificates"""

from requests.adapters import HTTPAdapter


class CustomHostnameCheckingAdapter(HTTPAdapter):
    """Verifies that the certificate matches the specified hostname"""

    def __init__(self, hostname):
        super().__init__()
        self._hostname = hostname

    def cert_verify(self, conn, url, verify, cert):
        conn.assert_hostname = self._hostname
        return super().cert_verify(conn, url, verify, cert)
