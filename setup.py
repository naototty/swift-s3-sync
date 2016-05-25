#!/usr/bin/python

from setuptools import setup

setup(name='swift-s3-sync',
      version='0.0.1',
      author='SwiftStack',
      test_suite='nose.collector',
      url='https://github.com/swiftstack/swift-s3-sync',
      packages=['s3_sync'],
      entry_points={
          'console_scripts': [
              'swift-s3-sync = s3_sync.__main__:main'
          ],
      })
