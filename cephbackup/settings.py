import ConfigParser
import os
import argparse
from distutils.util import strtobool
from ceph_backup import CephFullBackup


class Settings(object):

    def __init__(self, path):
        '''
        path: path to the configuration file
        '''
        super(Settings, self).__init__()
        self._path = path
        if not os.path.exists(path):
            raise Exception('Configuration file not found: {}'.format(path))
        self._config = ConfigParser.ConfigParser()
        self._config.read(self._path)

    def getsetting(self, section, setting):
        return self._config.get(section, setting)

    def start_backup(self):
        '''
        Read settings and starts backup
        '''
        for section in self._config.sections():
            # Run a backup for each section
            print "Starting backup for pool {}".format(section)
            images = self.getsetting(section, 'images').split(',')
            backup_dest = self.getsetting(section, 'destination directory')
            conf_file = self.getsetting(section, 'ceph config')
            check_mode = bool(strtobool(self.getsetting(section, 'check mode')))
            compress_mode = bool(strtobool(self.getsetting(section, 'compress')))
            window_size = int(self.getsetting(section, 'window size'))
            window_unit = self.getsetting(section, 'window unit')
            backup_mode = self.getsetting(section, 'backup mode')
            cb = CephFullBackup(section, images, backup_dest, conf_file, check_mode, compress_mode, window_size, window_unit)
            if backup_mode == 'full':
                print "Full ceph backup"
                cb.print_overview()
                cb.full_backup()
            elif backup_mode == 'incremental':
                print "Incremental ceph backup"
                cb.print_overview()
                cb.incremental_backup()
            else:
                raise Exception("Unknown backup mode: {}".format(backup_mode))
