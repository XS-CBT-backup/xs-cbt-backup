This repository contains a command-line program written in Python 3, `backup.py`, which can be used to back up and restore VMs on a XenServer host that supports Changed Block Tracking.

**NOTE: This is just a simple example program intended to demonstrate one way in which backup can be implemented using the Changed Block Tracking APIs of XenServer, and should not be used in production!**

## Usage

The CLI program has built-in help, which can be displayed by passing the `--help` argument to it or to one of its subcommands.

### Managing Backups

All backup data is stored inside the `~/.cbt_backups` directory. Inside this, each backed up VM has its own directory, which contains one subdirectory for each backup of that VM.
A specific backup of a VM, all backups of a VM, or all backups created by the program can be removed by deleting the corresponding folder.

The backup program tries to create shallow copies when possible, therefore the speed of incremental backups can be improved by placing the main backup directory on a copy-on-write filesystem that supports reflinks.

### Configuring TLS

By default, TLS is enabled. It can be disabled with the `--no-tls` option.

To make TLS work, make sure that the server's CA certificate is included in the CA bundle used by the [requests] Python library, which this program uses for HTTPS requests.
One way of setting this up on Ubuntu:

1. Add the server's CA certificate to the system ca-certificates bundle, as explained [here](https://github.com/xapi-project/xen-api/issues/2100#issuecomment-361930724)
2. Make sure that this CA bundle is used by the [requests] library by setting the corresponding env var, as specified in the [docs](http://docs.python-requests.org/en/master/user/advanced/#ssl-cert-verification):

   In bash, run:
   ```
   export REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt
   ```
   In case of the fish shell, run:
   ```
   set -x REQUESTS_CA_BUNDLE /etc/ssl/certs/ca-certificates.crt
   ```

## Possible Improvements:

* Download the initial disk as VHD and create a sparse file using `truncate`
* Upload as VHD reconstructed from sparse file using `SEEK_DATA`
* Quiesce VM? - in case backups of Windows VMs are unusable

[requests]: http://docs.python-requests.org/en/master/
