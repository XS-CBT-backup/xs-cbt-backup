"""
Module for computing checksums for validating backup / restore.
The VDI checksums and file checksums computed by the functions in this
module can be compared against each other, and they should match if the
contents are identical.
"""

import hashlib


def file_checksum(filepath):
    """
    Compute the MD5 checksum of the file.
    """
    with open(filepath, 'rb') as infile:
        hasher = hashlib.md5()
        while True:
            data = infile.read(65536)
            if not data:
                break
        hasher.update(data)
    return hasher.hexdigest()


def vdi_checksum(session, vdi):
    """
    Compute the checksum of the VDI.
    """
    return session.xenapi.VDI.checksum(vdi)
