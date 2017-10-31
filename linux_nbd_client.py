"""
A Python wrapper around the nbd-client native Linux program.
"""

import subprocess
import tempfile
import os


def _write_cert_to_file(cert):
    certfile = tempfile.NamedTemporaryFile()
    certfile.write(cert)
    return certfile.name


def is_nbd_device_connected(nbd_device):
    """
    Checks whether the specified nbd device is connected according to
    nbd-client.
    """
    cmd = ['nbd-client', '-c', nbd_device]
    returncode = subprocess.run(cmd).returncode
    if returncode == 0:
        return True
    if returncode == 1:
        return False
    raise subprocess.CalledProcessError(returncode=returncode, cmd=cmd)


class LinuxNbdClient(object):
    """
    Python code wrapping an nbd-client connection.
    """
    def __init__(self,
                 address,
                 exportname="",
                 port=10809,
                 subject=None,
                 cert=None,
                 use_tls=True,
                 nbd_device=None,
                 block_size=None,
                 timeout=None,
                 use_socket_direct_protocol=False,
                 persist=True):
        command = ['nbd-client', '-name', exportname,
                   address, port, nbd_device]
        if block_size is not None:
            command += ['-block-size', str(block_size)]
        if timeout is not None:
            command += ['-timeout', str(timeout)]
        if use_socket_direct_protocol:
            command += ['-sdp']
        if persist:
            command += ['-persist']
        if use_tls:
            certfile = _write_cert_to_file(cert)
            command += ['-cacertfile', certfile]
            if subject is not None:
                command += ['-tlshostname', subject]
            command += ['-enable-tls']

        print(command)
        subprocess.check_output(command)

        self.nbd_device = nbd_device

    def __del__(self):
        self.close()

    def close(self):
        """
        Issues sync request, and then sends a disconnect request to the server.
        """
        self.flush()
        self._disconnect()

    def read(self, offset, length):
        """
        Returns length number of bytes read from the export, starting at
        the given offset.
        """
        with open(self.nbd_device, 'rb') as fin:
            fin.seek(offset)
            fin.read(length)

    def write(self, offset, length):
        """
        Returns length number of bytes read from the export, starting at
        the given offset.
        """
        with open(self.nbd_device, 'rb+') as fin:
            fin.seek(offset)
            fin.read(length)

    def _disconnect(self):
        subprocess.check_output(['nbd-client', '-d', self.nbd_device])

    def flush(self):
        """
        Issues a sync request.
        """
        os.sync()
