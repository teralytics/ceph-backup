from setuptools import setup


setup(
    name='cephbackup',
    version='0.0.1',
    packages=['cephbackup',],
    install_requires=['executor'],
    entry_points={'console_scripts': ['cephbackup = cephbackup.__main__:main']},
    url='',
    license='',
    author='vincepii',
    author_email='vincenzo.pii@teralytics.net',
    description='Backup tool for Ceph volumes'
)
