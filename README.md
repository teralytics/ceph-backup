## cephbackup

A tool to take backups of ceph rados block images that works in two backup modes (see [Sample configuration](#sample-configuration)):

 * Incremental: incremental backups within a given backup window based on rbd snapshots
 * Full: full image exports without snapshots

**Note on consistency**: this tool makes snapshots of rbd images without any awareness of the status of the filesystem they contain. Be aware of the consistency limits of rbd snapshots: http://docs.ceph.com/docs/hammer/rbd/rbd-snapshot/.
Also, since "Full" mode doesn't use snapshots, backups of *live* images in "Full" mode are not consistent (on the contrary, "Incremental" mode always uses snapshots).

## Building

Generate the source distribution to be installed with pip

    $ python setup.py sdist

Or directly install the package

    $ python setup.py install

## Running

Create a configuration file and place it in `/etc/cephbackup/cephbackup.conf` (or specify the path of the configuration file with the `-c` option), then run the tool:

    $ sudo cephbackup

## Sample configuration

Defines a backup configuration for a single ceph pool called "rbd", with a window size of 7 days and incremental (diffs) backups.
Two images are configured for backup: `rbd/logs` and `rbd/conf`.
Exported backup files will be compressed.

    [rbd]
    window size = 7
    window unit = days
    destination directory = /mnt/ceph_backups/
    images = logs,conf
    compress = yes
    ceph config = /etc/ceph/ceph.conf
    backup mode = incremental
    check mode = no

## Restoring an incremental backup

Restore the base export (full):

    # rbd import config@UTC20161130T170848.full dest_image

Recreate the base snapshot on the restored image:

    # rbd snap create dest_image@UTC20161130T170848

Restore the incremental diffs:

    # rbd import-diff config@UTC20161130T170929.diff_from_UTC20161130T170848 dest_image
