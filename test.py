import XenAPI
from linux_nbd_client import LinuxNbdClient
from pprint import pprint as pp

s = XenAPI.Session("http://dt14")
s.xenapi.login_with_password("root", "xenroot")
x = s.xenapi

info = x.VDI.get_nbd_info(x.VDI.get_all()[30])[0]
pp(info)

client = LinuxNbdClient(**info, use_tls=False)
