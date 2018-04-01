This repository contains a command-line program written in Python 3, `backup.py`, which can be used to back up and restore VMs on a XenServer host that supports Changed Block Tracking.

**NOTE: This is just a simple example program intended to demonstrate one way in which backup can be implemented using the Changed Block Tracking APIs of XenServer, and should not be used in production!**

## Usage

The CLI program has built-in help, which can be displayed by passing the `--help` argument to it or to one of its subcommands.

### Managing Backups

All backup data is stored inside the `~/.cbt_backups` directory. Inside this, each backed up VM has its own directory, which contains one subdirectory for each backup of that VM.
A specific backup of a VM, all backups of a VM, or all backups created by the program can be removed by deleting the corresponding folder.

The backup program tries to create shallow copies when possible, therefore the speed of incremental backups can be improved by placing the main backup directory on a copy-on-write filesystem that supports reflinks.

## Possible Improvements:

* Download the initial disk as VHD and create a sparse file using `truncate`
* Upload as VHD reconstructed from sparse file using `SEEK_DATA`
* Quiesce VM? - in case backups of Windows VMs are unusable
