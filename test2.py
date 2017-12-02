from pprint import pprint as pp
import sys

import XenAPI

from python_nbd_client import PythonNbdClient


def test():
    session = XenAPI.Session("http://" + sys.argv[1])
    session.xenapi.login_with_password("root", "xenroot")
    xenapi = session.xenapi

    info = xenapi.VDI.get_nbd_info(xenapi.VDI.get_all()[30])[0]
    pp(info)

    with PythonNbdClient(**info, use_tls=False):
        pass


if __name__ == '__main__':
    test()
