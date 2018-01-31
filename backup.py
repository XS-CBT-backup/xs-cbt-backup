#!/usr/bin/env python3

import datetime
import logging
import shutil
import ssl
import time
import urllib.request
import xml.etree.ElementTree as ElementTree
from pathlib import Path

import XenAPI

from vdi_downloader import VdiDownloader
import md5sum

PROGRAM_NAME = "backup.py"


def get_vdis_of_vm(session, vm_ref):
    """
    Returns the non-empty VDIs that are connected to a VM by a plugged or
    unplugged VBD.
    """
    for vbd in session.xenapi.VM.get_VBDs(vm_ref):
        vdi = session.xenapi.VBD.get_VDI(vbd)
        if not session.xenapi.VBD.get_empty(vbd):
            yield vdi


def enable_cbt(session, vm_ref):
    """
    Enables CBT on all the VDIs of a VM.
    """
    for vdi in get_vdis_of_vm(session=session, vm_ref=vm_ref):
        session.xenapi.VDI.enable_cbt(vdi)


def _get_timestamp():
    # Avoid characters that are invalid in filenames.
    # ISO 8601
    return datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")


class BackupConfig(object):
    def __init__(self, session, backup_dir, vm_uuid, use_tls):
        self._session = session
        self._backup_dir = backup_dir
        self._vm = vm_uuid

        self._downloader = VdiDownloader(
            session=self._session,
            block_size=4 * 1024 * 1024,
            use_tls=use_tls)

        self._vm_uuid = vm_uuid
        self._vm = self._session.xenapi.VM.get_by_uuid(vm_uuid)
        self._vm_dir = self._backup_dir / vm_uuid
        self._vm_dir.mkdir(exist_ok=True)

    def _get_new_backup_dir(self):
        timestamp = _get_timestamp()
        backup_dir = self._vm_dir / timestamp
        backup_dir.mkdir()
        print("Created new backup directory {}".format(backup_dir))
        return backup_dir

    def _get_all_vdi_backups(self):
        for vm_backup in self._vm_dir.iterdir():
            for vdi_backup in vm_backup.iterdir():
                yield vdi_backup

    def _get_local_backup_of_snapshot(self, snapshot):
        uuid = self._session.xenapi.VDI.get_uuid(snapshot)
        local_backup = next((b for b in self._get_all_vdi_backups()
                             if b.name == uuid),
                            None)
        return local_backup

    def _snapshot_timestamp(self, snapshot):
        return self._session.xenapi.VDI.get_snapshot_time(snapshot)

    def _get_latest_backup_of_vdi(self, snapshot):
        # First we need to get the original VDI that we've just snapshotted
        # - the snapshots field of a snapshot VDI is empty.
        vdi = self._session.xenapi.VDI.get_snapshot_of(snapshot)
        snapshots = self._session.xenapi.VDI.get_snapshots(vdi)
        print("Found snapshots of VDI: {}".format(snapshots))
        snapshots_from_newest_to_oldest = sorted(
            snapshots, key=self._snapshot_timestamp, reverse=True)
        backups_from_newest_to_oldest = (
            (s, self._get_local_backup_of_snapshot(s))
            for s in snapshots_from_newest_to_oldest)
        backups_from_newest_to_oldest = (
            (s, b)
            for (s, b) in backups_from_newest_to_oldest
            if b is not None)
        return next(iter(backups_from_newest_to_oldest), None)

    def _compare_checksums(self, vdi, backup):
        print("Starting to checksum VDI on server side")
        task = self._session.xenapi.Async.VDI.checksum(vdi)
        print("Checksumming local backup")
        backup_checksum = md5sum.md5sum(backup)
        print("Waiting for server-side checksum to finish...")
        while self._session.xenapi.task.get_status(task) == "pending":
            time.sleep(1)
        checksum = ElementTree.fromstring(self._session.xenapi.task.get_result(task)).text
        print("Comparing checksums: local {} server {}".format(backup_checksum, checksum))
        assert backup_checksum == checksum

    def _vdi_backup(self, backup_dir, vdi):
        """
        Backs up a VDI of the newly-created VM snapshot and then cleans
        it up from the server. If CBT is enabled on the snapshot VDI,
        and there is a local backup of a snapshot in this snapshot chain,
        and incremental backup is performed. Otherwise, a full VDI
        backup is performed.
        """
        vdi_uuid = self._session.xenapi.VDI.get_uuid(vdi)
        print("Backing up VDI {} with UUID {}".format(vdi, vdi_uuid))
        output_file = backup_dir / vdi_uuid
        cbt_enabled = self._session.xenapi.VDI.get_cbt_enabled(vdi)

        latest_backup = None
        if cbt_enabled:
            latest_backup = self._get_latest_backup_of_vdi(vdi)
            print("Found latest backup: {}".format(latest_backup))
        if latest_backup is None:
            self._downloader.full_vdi_backup(
                vdi=vdi,
                output_file=output_file)
        else:
            self._downloader.incremental_vdi_backup(
                vdi=vdi,
                latest_backup=latest_backup,
                output_file=output_file)
        self._compare_checksums(vdi=vdi, backup=output_file)

    def _vm_backup(self, vm_snapshot, backup_dir):
        vdis = get_vdis_of_vm(self._session, vm_snapshot)

        # Back up the VDIs:
        for vdi in vdis:
            self._vdi_backup(backup_dir=backup_dir, vdi=vdi)


        # Remove the backed up data from the server:

        # The VM snapshot has to be removed before data_destroying the VDIs -
        # data_destroy isn't allowed if the VDI has any plugged or unplugged
        # VBDs, so as long as the VDI is linked to the VM snapshot by a VBD, we
        # cannot data_destroy it.
        self._session.xenapi.VM.destroy(vm_snapshot)
        for vdi in vdis:
            if self._session.xenapi.VDI.get_cbt_enabled(vdi):
                self._session.xenapi.VDI.data_destroy(vdi)
            else:
                self._session.xenapi.VDI.destroy(vdi)

    def _snapshot_vm(self):
        new_name = self._session.xenapi.VM.get_name_label(
            self._vm) + "_tmp_cbt_backup_snapshot"
        print("Snapshotting VM {} as snapshot '{}".format(self._vm, new_name))
        return self._session.xenapi.VM.snapshot(self._vm, new_name)

    def _save_vm_metadata(self, backup_dir):
        session_ref = self._session._session
        host = self._session.xenapi.session.get_this_host(session_ref)
        hostname = self._session.xenapi.host.get_hostname(host)
        cert = self._session.xenapi.host.get_server_certificate(host)
        url = "https://{}/export_metadata?session_id={}&uuid={}&export_snapshots=false".format(
                hostname, session_ref, self._vm_uuid)

        context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
        context.options &= ~ssl.OP_NO_TLSv1
        context.options &= ~ssl.OP_NO_TLSv1_1
        context.options &= ~ssl.OP_NO_SSLv2
        context.options &= ~ssl.OP_NO_SSLv3
        context.verify_mode = ssl.CERT_REQUIRED
        context.check_hostname = True
        context.load_verify_locations(cadata=cert)

        with urllib.request.urlopen(url=url, context=context) as response, (backup_dir / "VM_metadata").open('wb') as out:
            shutil.copyfileobj(response, out)

    def backup(self):
        """
        Takes a backup of the VM.
        """
        print("Backing up VM {}".format(self._vm))
        print(
            "Backups of VM {} are stored in {}".format(self._vm, self._vm_dir))
        enable_cbt(self._session, self._vm)
        backup_dir = self._get_new_backup_dir()
        self._save_vm_metadata(backup_dir)
        snapshot = self._snapshot_vm()
        self._vm_backup(vm_snapshot=snapshot, backup_dir=backup_dir)


def backup(master, vm, pwd, uname='root', tls=True):
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    session = XenAPI.Session("https://" + master)
    session.xenapi.login_with_password(
        uname, pwd, "1.0", PROGRAM_NAME)

    backup_dir = Path.home() / ".cbt_backups"

    backup_config = BackupConfig(
        session=session,
        backup_dir=backup_dir,
        vm_uuid=vm,
        use_tls=tls)

    backup_config.backup()


if __name__ == '__main__':
    import fire
    fire.Fire(backup)
