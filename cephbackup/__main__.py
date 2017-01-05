import sys
import argparse
from settings import Settings


def main():
    parser = argparse.ArgumentParser(description='Backup tool for ceph')
    default_cephbackup_cfg = '/etc/cephbackup/cephbackup.conf'
    parser.add_argument('-c', '--conf', help="path to the configuration file (default: {})".format(default_cephbackup_cfg), type=str, default=default_cephbackup_cfg)
    args = parser.parse_args()
    settings = Settings(args.conf)
    settings.start_backup()

if __name__ == "__main__":
    main()
