#!/usr/bin/python

from setuptools import setup

with open('requirements.txt') as reqs_file:
    reqs = [req.strip() for req in reqs_file]

setup(name='swift-s3-sync',
      version='0.1.6',
      author='SwiftStack',
      test_suite='nose.collector',
      url='https://github.com/swiftstack/swift-s3-sync',
      packages=['s3_sync'],
      install_requires=reqs,
      entry_points={
          'console_scripts': [
              'swift-s3-sync = s3_sync.__main__:main'
          ],
          'paste.filter_factory': [
              'cloud-shunt = s3_sync.shunt:filter_factory'
          ],
      })
