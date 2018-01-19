#!/usr/bin/env python3

from pathlib import Path

import XenAPI

from vdi_downloader import VdiDownloader
import md5sum

PROGRAM_NAME = "backup.py"


def get_vdis_of_vm(session, vm_ref):
    """
    Returns the non-empty VDIs that are connected to a VM by a plugged or unplugged VBD.
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
    import datetime
    # Avoid using characters that are invalid in filenames
    now = datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S")
    return now


class Backup(object):
    def __init__(
            self,
            pool_master_address,
            username,
            password,
            vm_uuid,
            use_tls=True):

        self._session = XenAPI.Session("http://" + pool_master_address)
        self._session.xenapi.login_with_password(
            username, password, "1.0", PROGRAM_NAME)

        self._backup_dir = Path.home() / ".cbt_backups"

        self._downloader = VdiDownloader(
            session=self._session,
            block_size=4 * 1024 * 1024,
            use_tls=use_tls)

        self._vm = self._session.xenapi.VM.get_by_uuid(vm_uuid)
        self._vm_dir = self._backup_dir / vm_uuid
        self._vm_dir.mkdir(exist_ok=True)

    def _get_backup_dirs(self):
        print(
            "Listing subdirectories of the VM backup directory {} "
            "corresponding to backups of that VM.".
            format(self._vm_dir))
        return self._vm_dir.iterdir()

    def _has_backup(self):
        return any(self._get_backup_dirs())

    def _get_new_backup_dir(self, timestamp):
        backup_dir = self._vm_dir / timestamp
        backup_dir.mkdir()
        print("Created new backup directory {}".format(backup_dir))
        return backup_dir

    def _get_all_vdi_backups(self):
        for backup in self._get_backup_dirs():
            for snapshot in backup.iterdir():
                print("Found backed up snapshot {} in directory {}".format(
                    snapshot.name, snapshot))
                yield snapshot

    def _get_local_backup_of_snapshot(self, snapshot):
        uuid = self._session.xenapi.VDI.get_uuid(snapshot)
        print(
            "Trying to find the local backup of snapshot VDI {} with UUID {}".
            format(snapshot, uuid))
        local_backup = next((b for b in self._get_all_vdi_backups()
                             if b.name == uuid), None)
        print("Found local backup {} for snapshot with UUID {}".format(
            local_backup, uuid))
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
        backups_from_newest_to_oldest = list(backups_from_newest_to_oldest)
        backups_from_newest_to_oldest = list(
            (s, b) for (s, b) in backups_from_newest_to_oldest
            if b is not None)
        return next(iter(backups_from_newest_to_oldest), None)

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
        print("Checksumming VDI on server side")
        checksum = md5sum.vdi_checksum(self._session, vdi)
        print("checksum: {}".format(checksum))
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
        print("Checksumming local backup")
        backup_checksum = md5sum.file_checksum(output_file)
        print("checksum: {}".format(backup_checksum))
        assert backup_checksum == checksum

        if cbt_enabled:
            self._session.xenapi.VDI.data_destroy(vdi)
        else:
            self._session.xenapi.VDI.destroy(vdi)

    def _vm_backup(self, vm_snapshot, backup_dir):
        for vdi in get_vdis_of_vm(self._session, vm_snapshot):
            self._vdi_backup(backup_dir=backup_dir, vdi=vdi)

    def _snapshot_vm(self, timestamp):
        new_name = self._session.xenapi.VM.get_name_label(
            self._vm) + "_cbt_backup_" + timestamp
        print("Snapshotting VM {} as snapshot '{}".format(self._vm, new_name))
        return self._session.xenapi.VM.snapshot(self._vm, new_name)

    def backup(self):
        """
        Takes a backup of the VM.
        """
        print("Backing up VM {}".format(self._vm))
        print(
            "Backups of VM {} are stored in {}".format(self._vm, self._vm_dir))
        enable_cbt(self._session, self._vm)
        timestamp = _get_timestamp()
        backup_dir = self._get_new_backup_dir(timestamp)
        snapshot = self._snapshot_vm(timestamp)

        self._vm_backup(vm_snapshot=snapshot, backup_dir=backup_dir)


if __name__ == '__main__':
    import fire
    fire.Fire(Backup)
