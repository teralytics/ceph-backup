#! /usr/bin/env python

from executor import execute
from datetime import datetime, timedelta
import argparse
import os
import rados
import rbd
import re


class CephFullBackup(object):
    '''
    Commands reference:
            # rbd export-diff --from-snap snap rbd/test@snap2 testexport
            # rbd export-diff --from-snap snap rbd/test testexport
            # rbd export test testexport
            # rbd export test@snap testexport
    '''

    PREFIX = 'BACKUP'
    TIMESTAMP_FMT = '{}UTC%Y%m%dT%H%M%S'.format(PREFIX)
    FULL_BACKUP_SUFFIX = '.full'
    DIFF_BACKUP_SUFFIX = '.diff_from'
    COMPRESSED_BACKUP_SUFFIX = '.tar.gz'

    def __init__(self, pool, images, backup_dest, conf_file, check_mode=False, compress_mode=False, window_size=7, window_unit='days'):
        '''
        images: list of images to backup
        backup_dest: path where to write the backups
        '''
        super(CephFullBackup, self).__init__()
        if len(set(images)) != len(images):
            raise Exception("Duplicated elements detected in list of images")
        self._pool = pool
        self._images = images
        self._backup_dest = backup_dest
        self._check_mode = check_mode
        self._compress_mode = compress_mode
        # TODO: support also cardinal backup window instead of temporal one, snapshots unit
        self._window_size = window_size
        self._window_unit = window_unit
        # Ceph objects
        cluster = rados.Rados(conffile=conf_file)
        cluster.connect()
        self._ceph_ioctx = cluster.open_ioctx(pool)
        self._ceph_rbd = rbd.RBD()

        # support wildcard for images
        if len(self._images) == 1 and self._images[0] == '*':
            self._images = self._get_images()

    def print_overview(self):
        print "Images to backup:"
        for image in self._images:
            print "\t{}/{}".format(self._pool, image)
        print "Backup folder: {}".format(self._backup_dest)
        print "Compression: {}".format(self._compress_mode)
        print "Check mode: {}".format(self._check_mode)

    def full_backup(self):
        '''
        Writes a full backup (not incremental) of each image.
        '''
        print "Taking full backup of images: {}".format(', '.join(self._images))
        if self._check_mode:
            print "Running in check mode: backup commands will just be printed and not executed"
        for image in self._images:
            timestamp = self._get_timestamp_str()
            fullsnapshotname = CephFullBackup._get_full_snapshot_name(image, timestamp)

            # Take snapshot
            self._create_snapshot(image, timestamp)

            # Export image
            self._export_image_or_snapshot(fullsnapshotname, image, base=None)

            # Delete Snapshot after export
            self._delete_snapshot(image, timestamp)

    def _get_images(self):
        '''
        Fetches a list of all images inside the pool.
        '''

        return self._ceph_rbd.list(self._ceph_ioctx)

    def _get_snapshots(self, imagename):
        '''
        Fetches a list of snapshots.
        Each snapshot is represented like the following:
        {'id': 40L, 'name': u'UTC20161117T164401', 'size': 21474836480L}
        '''

        prefix_length = len(CephFullBackup.PREFIX)

        image = rbd.Image(self._ceph_ioctx, imagename)
        snapshots = []
        for snapshot in image.list_snaps():
            # only return backup snapshots
            if snapshot.get('name')[0:prefix_length] == CephFullBackup.PREFIX:
                snapshots.append(snapshot)

        return snapshots

    def _get_oldest_snapshot(self, imagename):
        snapshots = self._get_snapshots(imagename)
        if len(snapshots) is 0:
            return None
        # The oldest snapshot is the one with the lowest ID
        return min(snapshots, key=lambda x: x['id'])

    def _get_newest_snapshot(self, imagename):
        snapshots = self._get_snapshots(imagename)
        if len(snapshots) is 0:
            return None
        # The newest snapshot is the one with the highest ID
        return max(snapshots, key=lambda x: x['id'])

    def _get_oldest_valid_snapshot(self, imagename, date_ref):
        '''
        Returns the oldest snapshot that is more recent than the provided date
        reference
        '''
        snapshots = self._get_snapshots(imagename)
        oldest_valid_snapshot = None
        for s in snapshots:
            if not self._is_outside_of_date_backup_window(date_ref, self._get_date_from_timestamp_str(s.get('name'))):
                # Found a snapshot inside the backup window, first found is oldest
                return s

    def _get_num_snapshosts(self, imagename):
        return len(self._get_snapshots(imagename))

    def _create_snapshot(self, imagename, snapshotname):
        image = rbd.Image(self._ceph_ioctx, imagename)
        # The snapshot id as seen in rbd commands will be image@snapshotname
        if not self._check_mode:
            image.create_snap(snapshotname)

    def _delete_snapshot(self, imagename, snapshotname):
        image = rbd.Image(self._ceph_ioctx, imagename)
        snapshotfull = self._get_full_snapshot_name(imagename, snapshotname)
        print "Deleting snapshot {pool}/{snap}".format(pool=self._pool, snap=snapshotfull)
        if not self._check_mode:
            image.remove_snap(snapshotname)

    def _delete_old_snapshots(self, image, timestamp):
        '''
        Deletes all snapshots outside the backup window

        timestamp: time reference to determine the validity of the backup window
        '''
        date_ref = self._get_date_from_timestamp_str(timestamp)
        for s in self._get_snapshots(image):
            s_date_ref = self._get_date_from_timestamp_str(s.get('name'))
            if self._is_outside_of_date_backup_window(date_ref, s_date_ref):
                # Delete this snapshot
                self._delete_snapshot(image, s.get('name'))

    def _export_image_or_snapshot(self, name, root_name, base=None):
        '''
        Exports an image or a snapshot with a given name.
        If a base is provided, it must be a snapshot, the export will be
        incremental to the base.

        name: name of the image or snapshot
        root_name: name of the root image (same as name if exporting an image and not a snapshot)
        base: base snapshot for diff exports. If None, a full export will be done
        '''
        suffix = CephFullBackup.DIFF_BACKUP_SUFFIX + "_{}".format(base) if base else CephFullBackup.FULL_BACKUP_SUFFIX
        timestamp = self._get_timestamp_str()
        dest_dir = os.path.join(self._backup_dest, self._pool, root_name)
        if not self._check_mode and not os.path.exists(dest_dir):
            os.makedirs(dest_dir)
        issnap = False
        if '@' in name:
            # We are exporting a snapshot, do not write the timestamp in the filename
            issnap = True
        dest = CephFullBackup._get_dest_backup_name(dest_dir, name, timestamp, suffix, issnap)
        if os.path.exists(dest):
            raise Exception("The destination file already exists!")
        # This a read-only command to validate ceph resources, can be executed even in check mode
        # Exception will be thrown if command fails
        execute('rbd info {pool}/{image}'.format(pool=self._pool, image=name), sudo=True)
        cmdlist = []
        if not base:
            # This is a full (non diff) export of the image or snapshot
            print "Exporting image {image} to {dest}".format(image=name, dest=dest)
            cmdlist.append('rbd export {pool}/{image} {dest}'.format(pool=self._pool, image=name, dest=dest))
        else:
            # Exporting diffs from a base image or snapshot
            # Validate also the base
            execute('rbd info {pool}/{base}'.format(pool=self._pool, base=name), sudo=True)
            print "Exporting diff {base} -> {image} to {dest}".format(base=base, image=name, dest=dest)
            cmdlist.append('rbd export-diff --from-snap {base} {pool}/{image} {dest}'.format(base=base, pool=self._pool, image=name, dest=dest))
        if self._compress_mode:
            print "Compress mode activated"
            cmdlist.append('tar Scvfz {dest}{compressed} {dest}'.format(dest=dest, compressed=CephFullBackup.COMPRESSED_BACKUP_SUFFIX))
            cmdlist.append('rm {dest}'.format(dest=dest))
        for cmd in cmdlist:
            print "# " + cmd
            if not self._check_mode:
                execute(cmd, sudo=True)

    def incremental_backup(self):
        '''
        Writes an incremental backup of each image and slides the backup window
        as necessary
        '''
        for image in self._images:
            self._incremental_backup_image(image)

    def _is_outside_of_date_backup_window(self, now, snapshot_date):
        '''
        Given a date object, examines the backup settings and determines if this
        date is outside of the backup window
        '''
        if self._window_unit == 'minutes':
            return now - snapshot_date > timedelta(minutes=self._window_size)
        elif self._window_unit == 'hours':
            return now - snapshot_date > timedelta(hours=self._window_size)
        elif self._window_unit == 'days':
            return now - snapshot_date > timedelta(days=self._window_size)
        elif self._window_unit == 'weeks':
            return now - snapshot_date > timedelta(weeks=self._window_size)
        elif self._window_unit == 'months':
            return now - snapshot_date > timedelta(months=self._window_size)
        else:
            raise Exception("Unknown value for backup window unit {}".format(self._window_unit))

    @staticmethod
    def _get_timestamp_str():
        return datetime.utcnow().strftime(CephFullBackup.TIMESTAMP_FMT)

    @staticmethod
    def _get_date_from_timestamp_str(timestamp_str):
        return datetime.strptime(timestamp_str, CephFullBackup.TIMESTAMP_FMT)

    @staticmethod
    def _get_full_snapshot_name(image, timestamp):
        return "{image}@{snapshot}".format(image=image, snapshot=timestamp)

    @staticmethod
    def _get_dest_backup_name(dest_dir, image, timestamp, suffix, issnap):
        '''
        Returns the name of a destination backup file.
        Note that if compression is enabled, this will be changed (e.g., by adding '.tar.gz')

        image: root image name or snapshot full id if the backup is that of a snapshot
        '''
        if issnap:
            return os.path.join(dest_dir, image + suffix)
        return os.path.join(dest_dir, image + "_" + timestamp + suffix)

    def _delete_child_exports(self, image, snapshotname):
        '''
        Deletes all the export files that originate from the given snapshot id.
        Deletion is recursive.
        '''
        base_backup_folder = os.path.join(self._backup_dest, self._pool, image)
        parent_snapshot_name = snapshotname
        match_string = '{}@(.*?){}_{}'.format(image, CephFullBackup.DIFF_BACKUP_SUFFIX, parent_snapshot_name)
        for filename in os.listdir(base_backup_folder):
            m = re.match(match_string, filename)
            if m:
                # Found a child of this parent
                complete_fname = os.path.join(base_backup_folder, filename)
                print "Removing file {}".format(complete_fname)
                if not self._check_mode:
                    os.remove(complete_fname)
                # Delete the child of this as well
                self._delete_child_exports(image, m.group(1))

    def _delete_export_file(self, filename, soft=False):
        '''
        Utility method that tries to delete a file with the given name or with
        the compressed suffix

        soft: if no file is found, raise an exception
        '''
        compressed_filename = filename + CephFullBackup.COMPRESSED_BACKUP_SUFFIX
        if os.path.exists(filename):
            print "Removing file {}".format(filename)
            if not self._check_mode:
                os.remove(filename)
        elif os.path.exists(compressed_filename):
            print "Removing file {}".format(compressed_filename)
            if not self._check_mode:
                os.remove(compressed_filename)
        elif not soft:
            raise Exception("Cannot find file for deletion: {f} or {c}".format(f=filename, c=compressed_filename))

    def _delete_old_exports(self, image, timestamp):
        '''
        Deletes the exported files of the image that are older than the given timestamp
        '''
        base_backup_folder = os.path.join(self._backup_dest, self._pool, image)
        date_ref = self._get_date_from_timestamp_str(timestamp)
        for export in os.listdir(base_backup_folder):
            m = re.match('{}@(.*?)[{}|{}]'.format(image, CephFullBackup.DIFF_BACKUP_SUFFIX, CephFullBackup.FULL_BACKUP_SUFFIX), export)
            if not m:
                print "WARNING: unexpected file in {base}: {fn}".format(base=base_backup_folder, fn=export)
                continue
            file_date = self._get_date_from_timestamp_str(m.group(1))
            if file_date < date_ref or (file_date == date_ref and CephFullBackup.DIFF_BACKUP_SUFFIX in export):
                complete_fname = os.path.join(base_backup_folder, export)
                print "Removing file {}".format(complete_fname)
                if not self._check_mode:
                    os.remove(complete_fname)

    def _incremental_backup_image(self, image):
        oldest_snapshot = self._get_oldest_snapshot(image)
        newest_snapshot = self._get_newest_snapshot(image)
        timestamp = self._get_timestamp_str()
        fullsnapshotname = CephFullBackup._get_full_snapshot_name(image, timestamp)
        # Take a new snapshot of this image
        self._create_snapshot(image, timestamp)
        if not oldest_snapshot:
            # This image had no snapshots, fully export the new one
            self._export_image_or_snapshot(fullsnapshotname, image, None)
        else:
            # There was already one or more snapshots of this image
            # We have to move the backup window and add a new increment
            # XXX strong assumption on snapshot names, if someone else does snapshots, this breaks
            oldest_snap_date = self._get_date_from_timestamp_str(oldest_snapshot.get('name'))
            if self._is_outside_of_date_backup_window(self._get_date_from_timestamp_str(timestamp), oldest_snap_date):
                # We need to slide the window
                # Find the oldest backup inside the window (worst case, the one we just made)
                # This snapshot will become the new base (subsequent ones are already building on this, so it's OK)
                new_base_snapshot = self._get_oldest_valid_snapshot(image, self._get_date_from_timestamp_str(timestamp))
                # Fully export this snapshot (it will be the new base)
                basesnapshotname = CephFullBackup._get_full_snapshot_name(image, new_base_snapshot.get('name'))
                self._export_image_or_snapshot(basesnapshotname, image, None)
                # Delete all the exports of backups before the new base
                self._delete_old_exports(image, new_base_snapshot.get('name'))
                # Delete all the snapshots outside of the window
                self._delete_old_snapshots(image, timestamp)
            if self._get_num_snapshosts(image) > 1:
                # Export the diff between the latest snapshot we just made and the one before that
                # We don't have to take this additional snapshot if we emptied the backup window
                # and now it's just made of a base snapshot
                self._export_image_or_snapshot(fullsnapshotname, image, newest_snapshot.get('name'))

def main():
    parser = argparse.ArgumentParser(description='Backs-up a list of ceph images (rbd volumes)')
    parser.add_argument('-p', '--pool', help="rados pool where the images belong", required=True)
    parser.add_argument('-i', '--images', nargs='+', help="List of ceph images to backup", required=True)
    parser.add_argument('-d', '--dest', help="Destination directory", required=True)
    parser.add_argument('-n', '--check', help="Check mode, show the commands, do not write a backup", action="store_true")
    parser.add_argument('-z', '--compress', help="Compress mode, it will compress each exported file and delete the original one", action="store_true")
    parser.add_argument('-c', '--ceph-conf', help="Path to ceph configuration file", type=str, default='/etc/ceph/ceph.conf')
    args = parser.parse_args()

    cb = CephFullBackup(args.pool, args.images, args.dest, args.ceph_conf, args.check, args.compress)
    cb.full_backup()

if __name__ == '__main__':
    main()
