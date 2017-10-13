#!/usr/bin/python3

from cbt_tests import CBTTests
import XenAPI


program_name = "backup.py"


class Backup(object):
    def __init__(self, pool_master_address, username, password, vm_uuid, use_tls=True):
        import urllib
        from pathlib import Path

        self._pool_master_address = pool_master_address
        self._username = username
        self._password = password

        self._session = XenAPI.Session("http://" + pool_master_address)
        self._session.xenapi.login_with_password(
            self._username, self._password, "1.0", program_name)

        self._backup_dir = Path.home() / ".cbt_backups"
        self._backup_dir.mkdir(exist_ok=True)

        # don't use characters that are invalid in filenames
        self._pool_master_dir = self._backup_dir / urllib.parse.quote(
            pool_master_address)
        self._pool_master_dir.mkdir(exist_ok=True)

        self._cbt_lib = CBTTests(session=self._session, use_tls=True)

        self._vm = self._session.xenapi.VM.get_by_uuid(vm_uuid)
        self._vm_dir = self._pool_master_dir / vm_uuid
        self._vm_dir.mkdir(exist_ok=True)

    def _get_backup_dirs(self):
        print(
            "Listing subdirectories of the VM backup directory {} corresponding to backups of that VM.".
            format(self._vm_dir))
        return self._vm_dir.iterdir()

    def _has_backup(self):
        return any(self._get_backup_dirs())

    def _get_vdis_of_vm(self, vm):
        for vbd in self._session.xenapi.VM.get_VBDs(vm):
            vdi = self._session.xenapi.VBD.get_VDI(vbd)
            print("Got VDI {} of VM {}".format(vdi, vm))
            if not self._session.xenapi.VBD.get_empty(vbd):
                yield vdi

    def _enable_cbt(self, vm):
        for vdi in self._get_vdis_of_vm(vm):
            print("Enabling CBT on VDI {} of VM {}".format(vdi, vm))
            self._session.xenapi.VDI.enable_cbt(vdi)

    def _get_timestamp(self):
        import datetime
        # don't use characters that are invalid in filenames
        now = datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S")
        return now

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

    def _get_local_backup_of_snapshot(self, s):
        uuid = self._session.xenapi.VDI.get_uuid(s)
        print(
            "Trying to find the local backup of snapshot VDI {} with UUID {}".
            format(s, uuid))
        local_backup = next((b for b in self._get_all_vdi_backups()
                             if b.name == uuid), None)
        print("Found local backup {} for snapshot with UUID {}".format(
            local_backup, uuid))
        return local_backup

    def _snapshot_timestamp(self, s):
        return self._session.xenapi.VDI.get_snapshot_time(s)

    def _get_latest_backup_of_vdi(self, snapshot):
        # First we need to get the original VDI that we've just snapshotted
        # - the snapshots field of a snapshot VDI is empty.
        vdi = self._session.xenapi.VDI.get_snapshot_of(snapshot)
        print(
            "Trying to find the latest local backup of VDI {} to back up its new snapshot {}".
            format(vdi, snapshot))
        snapshots = self._session.xenapi.VDI.get_snapshots(vdi)
        print("Found snapshots of VDI: {}".format(snapshots))
        snapshots_from_newest_to_oldest = sorted(
            snapshots, key=lambda s: self._snapshot_timestamp(s), reverse=True)
        backups_from_newest_to_oldest = (
            (s, self._get_local_backup_of_snapshot(s))
            for s in snapshots_from_newest_to_oldest)
        backups_from_newest_to_oldest = list(backups_from_newest_to_oldest)
        print("Found backups of snapshot {}: {}".format(snapshot, backups_from_newest_to_oldest))
        backups_from_newest_to_oldest = list((s, b) for (s, b) in backups_from_newest_to_oldest
              if b is not None)
        print("Present backups of snapshot {}: {}".format(snapshot, backups_from_newest_to_oldest))
        return next(iter(backups_from_newest_to_oldest), None)

    def _full_vdi_backup(self, vdi, output_file):
        print("Starting a full backup for VDI {}".format(vdi))
        self._cbt_lib.download_whole_vdi_using_nbd(vdi=vdi, path=output_file)

    def _incremental_vdi_backup(self, vdi, latest_backup, output_file):
        print("Starting an incremental backup for VDI {}".format(vdi))
        import shutil
        import import subprocess

        (vdi_from, vdi_from_backup) = latest_backup

        print("Copying from {} to {}".format(vdi_from_backup, output_file))
        try:
                subprocess.check_output(["cp", "--reflink", str(vdi_from_backup), str(output_file)])
        except:
                shutil.copy(src=str(vdi_from_backup), dst=str(output_file))

        self._cbt_lib.save_changed_blocks(
            vdi_from=vdi_from, vdi_to=vdi, output_file=output_file)

    def _vdi_backup(self, backup_dir, vdi):
        vdi_uuid = self._session.xenapi.VDI.get_uuid(vdi)
        print("Backing up VDI {} with UUID {}".format(vdi, vdi_uuid))
        output_file = backup_dir / vdi_uuid

        latest_backup = self._get_latest_backup_of_vdi(vdi)
        print("Found latest backup: {}".format(latest_backup))
        if latest_backup is None:
            self._full_vdi_backup(vdi=vdi, output_file=output_file)
        else:
            self._incremental_vdi_backup(
                vdi=vdi, latest_backup=latest_backup, output_file=output_file)

    def _vm_backup(self, vm, backup_dir):
        for vdi in self._get_vdis_of_vm(vm):
            self._vdi_backup(backup_dir=backup_dir, vdi=vdi)

    def _snapshot_vm(self, timestamp):
        new_name = self._session.xenapi.VM.get_name_label(
            self._vm) + "_cbt_backup_" + timestamp
        print("Snapshotting VM {} as snapshot '{}".format(self._vm, new_name))
        return self._session.xenapi.VM.snapshot(self._vm, new_name)

    def backup(self):
        print("Backing up VM {} in the pool with master {}".format(
            self._vm, self._pool_master_address))
        print(
            "Backups of VM {} are stored in {}".format(self._vm, self._vm_dir))

        # Check for any existing backup directories before we create the new one below
        if not self._has_backup():
            print("VM {} has no local backups".format(self._vm))
            self._enable_cbt(self._vm)
        else:
            print("VM {} already has a local backup, assuming CBT is already enabled.".format(self._vm))

        timestamp = self._get_timestamp()
        backup_dir = self._get_new_backup_dir(timestamp)
        snapshot = self._snapshot_vm(timestamp)

        self._vm_backup(vm=snapshot, backup_dir=backup_dir)


if __name__ == '__main__':
    import fire
    fire.Fire(Backup)
