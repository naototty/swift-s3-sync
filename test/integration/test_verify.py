import s3_sync.verify
from . import TestCloudSyncBase


class TestVerify(TestCloudSyncBase):
    def setUp(self):
        self.swift_container = self.s3_bucket = None
        for container in self.test_conf['containers']:
            if container['protocol'] == 'swift':
                self.swift_container = container['aws_bucket']
            else:
                self.s3_bucket = container['aws_bucket']
            if self.swift_container and self.s3_bucket:
                break

    def get_swift_tree(self):
        return [
            container['name'] + '/' + obj['name']
            for container in self.swift_dst.get_account()[1]
            for obj in self.swift_dst.get_container(container['name'])[1]]

    def test_swift_no_container(self):
        before = self.get_swift_tree()
        self.assertEqual(0, s3_sync.verify.main([
            '--protocol=swift',
            '--endpoint=' + self.SWIFT_CREDS['authurl'],
            '--username=' + self.SWIFT_CREDS['dst']['user'],
            '--password=' + self.SWIFT_CREDS['dst']['key'],
        ]))
        self.assertEqual(before, self.get_swift_tree())

    def test_swift_single_container(self):
        self.assertEqual(0, s3_sync.verify.main([
            '--protocol=swift',
            '--endpoint=' + self.SWIFT_CREDS['authurl'],
            '--username=' + self.SWIFT_CREDS['dst']['user'],
            '--password=' + self.SWIFT_CREDS['dst']['key'],
            '--bucket=' + self.swift_container,
        ]))

    def test_swift_all_containers(self):
        self.assertEqual(0, s3_sync.verify.main([
            '--protocol=swift',
            '--endpoint=' + self.SWIFT_CREDS['authurl'],
            '--username=' + self.SWIFT_CREDS['dst']['user'],
            '--password=' + self.SWIFT_CREDS['dst']['key'],
            '--bucket=/*',
        ]))

    def test_swift_bad_creds(self):
        msg = ('Invalid credentials. Please check the Access Key ID and '
               'Secret Access Key.')
        self.assertEqual(msg, s3_sync.verify.main([
            '--protocol=swift',
            '--endpoint=' + self.SWIFT_CREDS['authurl'],
            '--username=' + self.SWIFT_CREDS['dst']['user'],
            '--password=not-the-password',
            '--bucket=' + self.swift_container,
        ]))

    def test_swift_bad_container(self):
        actual = s3_sync.verify.main([
            '--protocol=swift',
            '--endpoint=' + self.SWIFT_CREDS['authurl'],
            '--username=' + self.SWIFT_CREDS['dst']['user'],
            '--password=' + self.SWIFT_CREDS['dst']['key'],
            '--bucket=does-not-exist',
        ])
        self.assertIn('404 Not Found', actual)
        self.assertTrue(actual.startswith(
            'Unexpected error validating credentials: Object PUT failed: '))

    def test_s3_no_bucket(self):
        self.assertEqual(0, s3_sync.verify.main([
            '--protocol=s3',
            '--endpoint=' + self.S3_CREDS['endpoint'],
            '--username=' + self.S3_CREDS['user'],
            '--password=' + self.S3_CREDS['key'],
        ]))

    def test_s3_single_bucket(self):
        self.assertEqual(0, s3_sync.verify.main([
            '--protocol=s3',
            '--endpoint=' + self.S3_CREDS['endpoint'],
            '--username=' + self.S3_CREDS['user'],
            '--password=' + self.S3_CREDS['key'],
            '--bucket=' + self.s3_bucket,
        ]))

    def test_s3_bad_creds(self):
        msg = ('Invalid credentials. Please check the Access Key ID and '
               'Secret Access Key.')
        self.assertEqual(msg, s3_sync.verify.main([
            '--protocol=s3',
            '--endpoint=' + self.S3_CREDS['endpoint'],
            '--username=' + self.S3_CREDS['user'],
            '--password=not-the-password',
            '--bucket=' + self.s3_bucket,
        ]))

    def test_s3_bad_bucket(self):
        msg = ("Unexpected error validating credentials: 'The specified "
               "bucket does not exist'")
        self.assertEqual(msg, s3_sync.verify.main([
            '--protocol=s3',
            '--endpoint=' + self.S3_CREDS['endpoint'],
            '--username=' + self.S3_CREDS['user'],
            '--password=' + self.S3_CREDS['key'],
            '--bucket=does-not-exist',
        ]))
