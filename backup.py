#!/usr/bin/python3

from cbt_tests import CBTTests


class Backup(object):
    def __init__(self, pool_master, username, password, vm_uuid, use_tls=True):
        import urllib
        from pathlib import Path

        self._backup_dir = Path.home() / ".cbt_backups"
        self._backup_dir.mkdir(exist_ok=True)

        self._pool_master = pool_master
        # don't use characters that are invalid in filenames
        self._pool_master_dir = self._backup_dir / urllib.parse.quote(
            pool_master)
        self._pool_master_dir.mkdir(exist_ok=True)

        self._cbt_lib = CBTTests(
            pool_master=pool_master,
            username=username,
            password=password,
            use_tls=True)
        self._session = self._cbt_lib._session

        self._vm = self._session.xenapi.VM.get_by_uuid(vm_uuid)
        self._vm_dir = self._pool_master_dir / vm_uuid
        self._vm_dir.mkdir(exist_ok=True)

        self._username = username
        self._password = password

    def _get_backup_dirs(self):
        return self._vm_dir.iterdir()

    def _has_backup(self):
        any(self._get_backup_dirs())

    def _get_vdis_of_vm(self):
        for vbd in self._session.xenapi.VM.get_VBDs(self._vm):
            vdi = self._session.xenapi.VBD.get_VDI(vbd)
            print("Got VDI {}".format(vdi))
            if not self._session.xenapi.VBD.get_empty(vbd):
                yield vdi

    def _enable_cbt(self):
        for vdi in self._get_vdis_of_vm():
            print("Enabling CBT on VDI {}".format(vdi))
            self._session.xenapi.VDI.enable_cbt(vdi)

    def _get_timestamp(self):
        import datetime
        # don't use characters that are invalid in filenames
        now = datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S")
        return now

    def _get_new_backup_dir(self, timestamp):
        backup_dir = self._vm_dir / timestamp
        backup_dir.mkdir()
        return backup_dir

    def _get_all_vdi_backups(self):
        for backup in self._get_backup_dirs():
            for snapshot in backup.iterdir():
                yield snapshot

    def _get_local_backup_of_snapshot(self, s):
        uuid = self._session.xenapi.VDI.get_uuid(s)
        next((b for b in self._get_all_vdi_backups() if b.name == uuid), None)

    def _snapshot_timestamp(self, s):
        return self._session.xenapi.VDI.get_snapshot_time(s)

    def _get_latest_backup_of_vdi(self, vdi):
        snapshots = self._session.xenapi.VDI.get_snapshots(vdi)
        snapshots_from_newest_to_oldest = sorted(
            snapshots, key=lambda s: self._snapshot_timestamp(s), reverse=True)
        backups_from_newest_to_oldest = (
            (s, self._get_local_backup_of_snapshot(s))
            for s in snapshots_from_newest_to_oldest)
        next(((s, b) for (s, b) in backups_from_newest_to_oldest
              if b is not None), None)

    def _full_vdi_backup(self, vdi, output_file):
        self._cbt_lib.download_whole_vdi_using_nbd(
            vdi=vdi, path=output_file)

    def _incremental_vdi_backup(self, vdi, latest_backup, output_file):
        import shutil

        (vdi_from, vdi_from_backup) = latest_backup

        shutil.copy(src=str(vdi_from_backup), dst=str(output_file))

        self._cbt_lib.save_changed_blocks(
            vdi_from=vdi_from, vdi_to=vdi, output_file=output_file)

    def _vdi_backup(self, backup_dir, vdi):
        vdi_uuid = self._session.xenapi.VDI.get_uuid(vdi)
        output_file = backup_dir / vdi_uuid

        latest_backup = self._get_latest_backup_of_vdi(vdi)
        if latest_backup is None:
            self._full_vdi_backup(vdi=vdi, output_file=output_file)
        else:
            self._incremental_vdi_backup(
                vdi=vdi, latest_backup=latest_backup, output_file=output_file)

    def _vm_backup(self, vm, backup_dir):
        for vdi in self._get_vdis_of_vm():
            self._vdi_backup(backup_dir=backup_dir, vdi=vdi)

    def _snapshot_vm(self, timestamp):
        new_name = self._session.xenapi.VM.get_name_label(
            self._vm) + "_cbt_backup_" + timestamp
        return self._session.xenapi.VM.snapshot(self._vm, new_name)

    def backup(self):
        timestamp = self._get_timestamp()
        backup_dir = self._get_new_backup_dir(timestamp)
        snapshot = self._snapshot_vm(timestamp)

        if not self._has_backup():
            self._enable_cbt()
        self._vm_backup(vm=snapshot, backup_dir=backup_dir)


if __name__ == '__main__':
    import fire
    fire.Fire(Backup)
