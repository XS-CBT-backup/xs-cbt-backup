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

        self._cbt_lib = CBTTests(pool_master=pool_master, username=username, password=password, use_tls=True)
        self._session = self._cbt_lib._session

        self.init_backup_dir()

        pool_master = pool_master or self.guess_pool_master()
        self._pool_master = pool_master
        self._pool_master_dir = self._backup_dir / urllib.parse.quote(pool_master)

        vm_uuid = vm_uuid or guess_vm_uuid()
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

    def get_backup_dirs_from_newest_to_oldest(self):
        sorted(self._vm_dir.iterdir(), reverse=True)

    def get_last_backup_dir(self):
        backups = self.get_backups_from_newest_to_oldest()
        return backups[0] if backups else None

    def backup(self):
        last_backup_dir = self.get_last_backup_dir()
        if last_backup is None:
