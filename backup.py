from cbt_tests import CBTTests

def get_only_child(self, path):
    children = path.iterdir()
    default = next(children)
    other = next(children, None)
    assert(other is None)
    return default

class Backup:
    def __init__(self, pool_master=None, username=None, password=None, vm_uuid=None, use_tls=True):
        import urllib

        self.init_backup_dir()

        pool_master = pool_master or self.guess_pool_master()
        self._pool_master = pool_master
        # don't use characters that are invalid in filenames
        self._pool_master_dir = self._backup_dir / urllib.parse.quote(pool_master)

        self._cbt_lib = CBTTests(pool_master=pool_master, username=username, password=password, use_tls=True)
        self._session = self._cbt_lib._session

        vm_uuid = vm_uuid or self.guess_vm_uuid()
        [self._vm] = self._session.xenapi.VM.get_by_uuid(vm_uuid)
        self._vm_dir = self._pool_master_dir / vm_uuid

        username = username or os.environ['XS_USERNAME']
        password = password or os.environ['XS_PASSWORD']
        self._username = username
        self._password = password

    def init_backup_dir(self):
        from pathlib import Path
        self._backupdir = Path.home() / ".cbt_backups"
        self._backupdir.mkdir(exist_ok=True)

    def guess_pool_master(self):
        import urllib
        return urllib.parse.unquote(get_only_child(self._backupdir))

    def guess_vm_uuid(self):
        return get_only_child(self._pool_master_dir)

    def get_backup_dirs(self):
        self._vm_dir.iterdir()

    def has_backup(self):
        any(self.get_backup_dirs())

    def enable_cbt(self):
        for vdi in self._session.xenapi.VM.get_VDIs(self._vm):
            self._session.xenapi.VDI.enable_cbt(vdi)

    def get_new_backup_dir(self):
        # don't use characters that are invalid in filenames
        now = datetime.datetime.utcnow().isoformat().strftime("%Y%m%d%H%M%S")
        backup_dir = self._vm_dir / now
        return backup_dir

    def full_vm_backup(self):
        import datetime

        snapshot = self._session.xenapi.VM.snapshot(self._vm)

        backup_dir = self.get_new_backup_dir()
        for vdi in self._session.xenapi.VM.get_VDIs(snapshot):
            vdi_uuid = self._session.xenapi.VDI.get_uuid(vdi)
            path = backup_dir / vdi_uuid
            self._cbt_lib.download_whole_vdi_using_nbd(vdi=vdi, filename=path)

    def get_all_vdi_backups(self):
        for backup in self.get_backup_dirs():
            for snapshot in backup.iterdir():
                yield snapshot

    def get_local_backup_of_snapshot(self, s):
        uuid = self._session.xenapi.VDI.get_uuid(s)
        next( (b for b in self.get_all_vdi_backups() if b.name == uuid), None)

    def snapshot_timestamp(self, s):
        return self._session.xenapi.VDI.get_snapshot_time(s)

    def get_latest_backup_of_vdi(self, vdi):
        snapshots = self._session.xenapi.VDI.get_snapshots(vdi)
        snapshots_from_newest_to_oldest = sorted(snapshots, key=lambda s: self.snapshot_timestamp(s), reverse=True)
        backups_from_newest_to_oldest = ((s, self.get_local_backup_of_snapshot(s)) for s in snapshots_from_newest_to_oldest)
        next( ((s, b) for (s, b) in backups_from_newest_to_oldest if b is not None), None)

    def incremental_vdi_backup(self, backup_dir, vdi):
        import shutil

        (vdi_from, vdi_from_backup) = self.get_latest_backup_of_vdi(vdi)
        vdi_uuid = self._session.xenapi.VDI.get_uuid(vdi)

        out = backup_dir / vdi_uuid
        shutil.copy(src=str(vdi_from_backup), dst=str(out))
        self._cbt_lib.save_changed_blocks(vdi_from=vdi_from, vdi_to=vdi_to, out)

    def incremental_vm_backup(self):
        backup_dir = self.get_new_backup_dir()
        new_name = self.xenapi.VM.get_name_label(self._vm) + "_cbt_backup_" + now
        snapshot = self._session.xenapi.VM.snapshot(self._vm, new_name)
        for vdi in self._session.xenapi.VM.get_VDIs(self._vm):
            incremental_vdi_backup(backup_dir, vdi)

    def backup(self):
        if not self.has_backup():
            self.enable_cbt()
            self.full_vm_backup()
        else:
            self.incremental_vm_backup()
