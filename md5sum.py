"""
Module for computing checksums for validating backup / restore.
"""

import hashlib
from pathlib import Path


def md5sum(filepath):
    """
    Compute the MD5 checksum of the file.  This can be computed against the
    output of VDI.checksum, and they should match if the contents are
    identical.
    """
    with Path(filepath).open('rb') as infile:
        hasher = hashlib.md5()
        while True:
            data = infile.read(65536)
            if not data:
                break
            hasher.update(data)
    return hasher.hexdigest()
