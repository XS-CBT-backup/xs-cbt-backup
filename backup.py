#!/usr/bin/env python3

# Requests should be configured to use the system ca-certificates bundle:
# * https://stackoverflow.com/questions/42982143
# * http://docs.python-requests.org/en/master/user/advanced/#ssl-cert-verification
# For example, run
# "export REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt" on Ubuntu.

from pathlib import Path
import argparse
import datetime
import logging
import os
import shutil
import time
import xml.etree.ElementTree as ElementTree

import XenAPI

from cbt_bitmap import CbtBitmap
from vdi_downloader import VdiDownloader
import md5sum
import verify

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


def vdi_supports_cbt(session, vdi):
    # For now, we cannot use the VDI's allowed_operations, because the CBT
    # opeartions aren't yet included
    sr = session.xenapi.VDI.get_SR(vdi)
    required_operations = set(['vdi_enable_cbt', 'vdi_list_changed_blocks', 'vdi_data_destroy'])
    allowed_operations = set(session.xenapi.SR.get_allowed_operations(sr))
    return required_operations.issubset(allowed_operations)


def enable_cbt(session, vm_ref):
    """
    Enables CBT on all the VDIs of a VM.
    """
    for vdi in get_vdis_of_vm(session=session, vm_ref=vm_ref):
        if vdi_supports_cbt(session=session, vdi=vdi):
            session.xenapi.VDI.enable_cbt(vdi)
        else:
            print('VDI {} does not support Changed Bloct Tracking'.format(
                session.xenapi.VDI.get_uuid(vdi)))


def _compare_checksums(session, vdi, backup):
    print("Starting to checksum VDI on server side")
    task = session.xenapi.Async.VDI.checksum(vdi)
    print("Checksumming local backup")
    backup_checksum = md5sum.md5sum(backup)
    print("Waiting for server-side checksum to finish...")
    checksum = _wait_for_task_result(session=session, task=task)
    assert backup_checksum == checksum


def restore_vdi(session, use_tls, host, sr, backup):
    """
    Returns a new VDI with the data taken from the backup.
    """
    size = os.path.getsize(str(backup))
    print('Creating VDI of size {}'.format(size))
    vdi_record = {
        'SR': sr,
        # ints are 64-bit and encoded as string in the XenAPI:
        'virtual_size': str(size),
        'type': 'user',
        'sharable': False,
        'read_only': False,
        'other_config': {},
        'name_label': 'Restored from CBT backup'
    }
    restored_vdi = session.xenapi.VDI.create(vdi_record)

    s = verify.session_for_host(session, host)

    address = session.xenapi.host.get_address(host)
    protocol = 'https' if use_tls else 'http'
    url = '{}://{}/import_raw_vdi?session_id={}&vdi={}&format=raw'.format(
            protocol, address, session._session, restored_vdi)

    with Path(backup).open('rb') as f:
        s.put(url, data=f).raise_for_status()

    _compare_checksums(session=session, vdi=restored_vdi, backup=backup)

    return restored_vdi


def _get_timestamp():
    # Avoid characters that are invalid in filenames.
    # ISO 8601
    return datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")


def _wait_for_task_to_finish(session, task):
    while session.xenapi.task.get_status(task) == "pending":
        time.sleep(1)


def _wait_for_task_result(session, task):
    _wait_for_task_to_finish(session=session, task=task)
    task_record = session.xenapi.task.get_record(task)
    assert task_record['status'] == 'success'
    element = ElementTree.fromstring(task_record['result'])
    value = next((child
                  for child
                  in element.iter('value')
                  if child.text is not None))
    return value.text


def _save_vm_metadata(session, use_tls, vm_uuid, backup_dir):
    session_ref = session._session
    host = session.xenapi.session.get_this_host(session_ref)
    address = session.xenapi.host.get_address(host)
    protocol = 'https' if use_tls else 'http'
    url = ('{}://{}/export_metadata'
           '?session_id={}'
           '&uuid={}'
           '&export_snapshots=false').format(
            protocol, address, session_ref, vm_uuid)

    s = verify.session_for_host(session, host)

    r = s.get(url)
    with (backup_dir / "VM_metadata").open('wb') as out:
        out.write(r.content)


class BackupConfig(object):
    def __init__(self, session, backup_dir, use_tls):
        self._session = session
        self._use_tls = use_tls

        self._backup_dir = backup_dir

        self._downloader = VdiDownloader(
            session=self._session,
            block_size=4 * 1024 * 1024,
            use_tls=use_tls)

    def _get_vm_dir(self, vm_uuid):
        vm_dir = self._backup_dir / vm_uuid
        vm_dir.mkdir(parents=True, exist_ok=True)
        return vm_dir

    def _get_local_backup_of_snapshot(self, snapshot):
        uuid = self._session.xenapi.VDI.get_uuid(snapshot)
        glob = '**/{}/data'.format(uuid)
        return next(self._backup_dir.glob(glob), None)

    def _snapshot_timestamp(self, snapshot):
        return self._session.xenapi.VDI.get_snapshot_time(snapshot)

    def _get_latest_backup_of_vdi(self, snapshot):
        # First we need to get the original VDI that we've just snapshotted
        # - the snapshots field of a snapshot VDI is empty.
        vdi = self._session.xenapi.VDI.get_snapshot_of(snapshot)
        snapshots = self._session.xenapi.VDI.get_snapshots(vdi)
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
        latest_backup = None
        if self._session.xenapi.VDI.get_cbt_enabled(vdi):
            latest_backup = self._get_latest_backup_of_vdi(vdi)

        vdi_dir = backup_dir / "vdis" / vdi_uuid
        vdi_dir.mkdir(parents=True)

        # First backup the UUID of the snapshotted VDI, because we save and
        # restore the metadata of the original VM, not the snapshot VM, and
        # therefore we have to specify the UUIDs of the snapshotted VM's VDIs
        # in the VDI mapping when we restore the VM from its metadata.
        with (vdi_dir / "original_uuid").open('w') as out:
            original_vdi = self._session.xenapi.VDI.get_snapshot_of(vdi)
            original_uuid = self._session.xenapi.VDI.get_uuid(original_vdi)
            out.write(original_uuid)

        # Then backup the data of the snapshot VDI
        output_file = vdi_dir / "data"
        if latest_backup is None:
            print("Performing a full backup")
            self._downloader.full_vdi_backup(
                vdi=vdi,
                output_file=output_file)
        else:
            print("Performing an incremental backup")
            changed_blocks = self._session.xenapi.VDI.list_changed_blocks(
                    latest_backup[0], vdi)
            stats = CbtBitmap(changed_blocks).get_statistics()
            print("Stats: {}".format(stats))
            self._downloader.incremental_vdi_backup(
                vdi=vdi,
                latest_backup=latest_backup,
                output_file=output_file)
        _compare_checksums(session=self._session, vdi=vdi, backup=output_file)

    def _vm_backup(self, vm_snapshot, backup_dir):
        vdis = list(get_vdis_of_vm(self._session, vm_snapshot))

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

    def _snapshot_vm(self, vm):
        new_name = self._session.xenapi.VM.get_name_label(
            vm) + "_tmp_cbt_backup_snapshot"
        print("Snapshotting VM")
        return self._session.xenapi.VM.snapshot(vm, new_name)

    def backup(self, vm_uuid):
        """
        Takes a backup of the VM.
        """
        vm = self._session.xenapi.VM.get_by_uuid(vm_uuid)

        vm_dir = self._get_vm_dir(vm_uuid)
        timestamp = _get_timestamp()
        backup_dir = vm_dir / timestamp
        backup_dir.mkdir()
        print("Backup up VM into new backup directory {}".format(backup_dir))
        try:

            enable_cbt(self._session, vm)

            snapshot = self._snapshot_vm(vm=vm)
            snapshot_uuid = self._session.xenapi.VM.get_uuid(snapshot)

            _save_vm_metadata(session=session, use_tls=self._use_tls, vm_uuid=vm_uuid, backup_dir=backup_dir)

            self._vm_backup(vm_snapshot=snapshot, backup_dir=backup_dir)

            return timestamp
        except:
            shutil.rmtree(backup_dir)
            raise

    def restore(self, vm_uuid, timestamp, sr, host):
        backup_dir = self._get_vm_dir(vm_uuid) / timestamp
        vdi_map = {}
        vm_metadata = backup_dir / "VM_metadata"
        for backup in (backup_dir / "vdis").iterdir():
            restored = restore_vdi(
                    session=self._session, use_tls=self._use_tls, host=host, sr=sr, backup=(backup/'data'))
            with (backup / "original_uuid").open('r') as infile:
                original_uuid = infile.readline().strip()
            restored_uuid = self._session.xenapi.VDI.get_uuid(restored)
            vdi_map[original_uuid] = restored_uuid
        vdi_map_params = ""
        for original_uuid, restored_uuid in vdi_map.items():
            vdi_map_params += "&vdi:{}={}".format(original_uuid, restored_uuid)

        address = self._session.xenapi.host.get_address(host)
        task = self._session.xenapi.task.create(
                "restore VM", "restore backed up VM metadata")
        s = verify.session_for_host(self._session, host)

        protocol = 'https' if self._use_tls else 'http'
        url = '{}://{}/import_metadata?session_id={}&task_id={}{}'.format(
            protocol, address, self._session._session, task, vdi_map_params)
        with vm_metadata.open('rb') as f:
            s.put(url, data=f).raise_for_status()

        vm = _wait_for_task_result(session=self._session, task=task)
        print('restored VM {}'.format(self._session.xenapi.VM.get_uuid(vm)))
        return vm


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    parser = argparse.ArgumentParser(description="Back up and restore VMs using XenServer's Changed Block Tracking API")
    parser.add_argument('--master', required=True, help="Address of the pool master")
    parser.add_argument('--pwd', required=True, help="Password of the user")
    parser.add_argument('--uname', default='root', help="Login name of the user")
    parser.add_argument('--tls', dest='tls', action='store_true')
    parser.add_argument('--no-tls', dest='tls', action='store_false')
    parser.set_defaults(tls=True)

    subparsers = parser.add_subparsers(dest='command_name')

    backup_parser = subparsers.add_parser('backup')
    backup_parser.add_argument('--vm', required=True, help="The UUID of the VM on the server to back up")

    backup_parser = subparsers.add_parser('restore')
    backup_parser.add_argument('--vm', required=True, help="The UUID of the locally backed up VM, which is to be restored")
    backup_parser.add_argument('--ts', required=True, help="The backup timestamp specifying which local backup of the VM to restore")
    backup_parser.add_argument('--sr', required=True, help="The SR on which the VDIs of the restored VM will be stored")
    backup_parser.add_argument('--host', required=True, help="The host through which the network traffic should travel while restoring the VM")

    args = parser.parse_args()

    session = XenAPI.Session(("https://" if args.tls else "http://") + args.master)
    session.xenapi.login_with_password(
        args.uname, args.pwd, "1.0", PROGRAM_NAME)
    try:
        backup_dir = Path.home() / ".cbt_backups"
        config = BackupConfig(
            session=session,
            backup_dir=backup_dir,
            use_tls=args.tls)
        if args.command_name == 'backup':
            print(config.backup(vm_uuid=args.vm))
        elif args.command_name == 'restore':
            sr = session.xenapi.SR.get_by_uuid(args.sr)
            host = session.xenapi.host.get_by_uuid(args.host)
            print(config.restore(vm_uuid=args.vm, timestamp=args.ts, sr=sr, host=host))
    finally:
        session.xenapi.logout()
