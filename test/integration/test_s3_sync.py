import boto3
import botocore.exceptions
import hashlib
import json
import os
import subprocess
import swiftclient
import time
import unittest


def clear_swift_container(client, container):
    _, list_results = client.get_container(container)
    for obj in list_results:
        client.delete_object(container, obj['name'])


def wait_for_condition(timeout, checker):
    start = time.time()
    while time.time() < start + timeout:
        ret = checker()
        if ret:
            return ret
        time.sleep(0.1)
    raise RuntimeError('Timeout expired')


def s3_prefix(account, container, key):
    md5_prefix = hashlib.md5('%s/%s' % (account, container))
    return hex(long(md5_prefix.hexdigest(), 16) % 16**6)[2:-1]


def s3_key_name(mapping, key):
    prefix = s3_prefix(
        mapping['account'],
        mapping['container'],
        key)
    return '%s/%s/%s/%s' % (
        prefix, mapping['account'], mapping['container'], key)


class TestCloudSync(unittest.TestCase):
    IMAGE_NAME = 'cloud-sync/test'
    PORTS = {}
    if 'TEST_CONTAINER' in os.environ:
        container = os.environ['TEST_CONTAINER']
    else:
        cmd = 'docker ps -f ancestor=%s -f status=running '\
              '--format "{{.Names}}"' % IMAGE_NAME
        images = subprocess.check_output(cmd.split())
        if not images:
            raise RuntimeError('Cannot find container from image %s' %
                               IMAGE_NAME)
        container = images.split()[0][1:-1]

    cmd = 'docker port %s' % container
    try:
        for line in subprocess.check_output(cmd.split()).split('\n'):
            if not line.strip():
                continue
            docker, host = line.split(' -> ')
            docker_port = int(docker.split('/')[0])
            host_port = int(host.split(':')[1])
            if docker_port == 8080:
                PORTS['swift'] = host_port
            elif docker_port == 10080:
                PORTS['s3'] = host_port
    except subprocess.CalledProcessError as e:
        print e.output
        print e.retcode

    CLOUD_SYNC_CONF = os.path.join(
        os.path.dirname(__file__), '../container/swift-s3-sync.conf')
    SWIFT_CREDS = {
        'authurl': 'http://localhost:%d/auth/v1.0' % PORTS['swift'],
        'src': {
            'user': 'test:tester',
            'key': 'testing',
        },
        'dst': {
            'user': 'test2:tester2',
            'key': 'testing2'
        }
    }

    @classmethod
    def setUpClass(self):
        self.test_conf = self._get_s3_sync_conf()
        self.swift_src = swiftclient.client.Connection(
            self.SWIFT_CREDS['authurl'],
            self.SWIFT_CREDS['src']['user'],
            self.SWIFT_CREDS['src']['key'])
        self.swift_dst = swiftclient.client.Connection(
            self.SWIFT_CREDS['authurl'],
            self.SWIFT_CREDS['dst']['user'],
            self.SWIFT_CREDS['dst']['key'])
        s3 = [container for container in self.test_conf['containers']
              if container['protocol'] == 's3'][0]
        session = boto3.session.Session(
            aws_access_key_id=s3['aws_identity'],
            aws_secret_access_key=s3['aws_secret'])
        conf = boto3.session.Config(s3={'addressing_style': 'path'})
        self.s3_client = session.client(
            's3', config=conf,
            endpoint_url='http://localhost:%d' % self.PORTS['s3'])

        for container in self.test_conf['containers']:
            if container['protocol'] == 'swift':
                self.swift_dst.put_container(container['aws_bucket'])
            else:
                try:
                    self.s3_client.create_bucket(
                        Bucket=container['aws_bucket'])
                except botocore.exceptions.ClientError as e:
                    if e.response['Error']['Code'] == 409:
                        pass
            self.swift_src.put_container(container['container'])

    @classmethod
    def tearDownClass(self):
        if 'NO_TEARDOWN' in os.environ:
            return
        for container in self.test_conf['containers']:
            if container['protocol'] == 'swift':
                self._remove_swift_container(
                    self.swift_dst, container['aws_bucket'])
            else:
                try:
                    list_results = self.s3_client.list_objects(
                        Bucket=container['aws_bucket'])
                    for obj in list_results.get('Contents', []):
                        self.s3_client.delete_object(
                            Bucket=container['aws_bucket'],
                            Key=obj['Key'])
                except botocore.exceptions.ClientError as e:
                    if e.response['Error']['Code'] == 'NoSuchBucket':
                        continue
                self.s3_client.delete_bucket(Bucket=container['aws_bucket'])

        for container in self.test_conf['containers']:
            self._remove_swift_container(
                self.swift_src, container['container'])

        for client in [self.swift_src, self.swift_dst]:
            if client:
                client.close()

    @classmethod
    def _get_s3_sync_conf(self):
        with open(self.CLOUD_SYNC_CONF) as conf_handle:
            conf = json.load(conf_handle)
            return conf

    @staticmethod
    def _remove_swift_container(client, container):
        clear_swift_container(client, container)
        client.delete_container(container)

    @classmethod
    def _find_mapping(klass, matcher):
        for mapping in klass.test_conf['containers']:
            if matcher(mapping):
                return mapping
        raise RuntimeError('No matching mapping')

    @classmethod
    def s3_sync_mapping(klass):
        return klass._find_mapping(
            lambda cont: cont['protocol'] == 's3' and cont['retain_local'])

    @classmethod
    def s3_archive_mapping(klass):
        return klass._find_mapping(
            lambda cont: cont['protocol'] == 's3' and not cont['retain_local'])

    @classmethod
    def swift_sync_mapping(klass):
        return klass._find_mapping(
            lambda cont: cont['protocol'] == 'swift' and cont['retain_local'])

    @classmethod
    def swift_archive_mapping(klass):
        return klass._find_mapping(
            lambda cont: cont['protocol'] == 'swift' and
            not cont['retain_local'])

    def local_swift(self, method, *args, **kwargs):
        return getattr(self.__class__.swift_src, method)(*args, **kwargs)

    def remote_swift(self, method, *args, **kwargs):
        return getattr(self.__class__.swift_dst, method)(*args, **kwargs)

    def s3(self, method, *args, **kwargs):
        return getattr(self.__class__.s3_client, method)(*args, **kwargs)

    def _test_archive(
            self, key, content, mapping, get_etag, expected_location):
        etag = self.local_swift(
            'put_object', mapping['container'], key,
            content.encode('utf-8'))
        self.assertEqual(
            hashlib.md5(content.encode('utf-8')).hexdigest(), etag)

        def _check_expired():
            # wait for the shunt to return the results for the object
            hdrs, listing = self.local_swift(
                'get_container', mapping['container'])
            if int(hdrs['x-container-object-count']) != 0:
                return False
            if any(map(lambda entry: 'content_location' not in entry,
                       listing)):
                return False
            return (hdrs, listing)

        swift_hdrs, listing = wait_for_condition(5, _check_expired)
        for entry in listing:
            if entry['name'] == key:
                break

        self.assertEqual(0, int(swift_hdrs['x-container-object-count']))
        self.assertEqual(etag, entry['hash'])
        self.assertEqual(
            expected_location,
            entry['content_location'])
        self.assertEqual(etag, get_etag(key))

    def test_s3_sync(self):
        s3_mapping = self.s3_sync_mapping()

        test_args = [
            (u'test_sync', u'testing archive put'),
            (u'unicod\u00e9', u'unicod\u00e9 blob')]
        for key, content in test_args:
            etag = self.local_swift(
                'put_object', s3_mapping['container'], key,
                content.encode('utf-8'))
            self.assertEqual(hashlib.md5(content.encode('utf-8')).hexdigest(),
                             etag)
            s3_key = s3_key_name(s3_mapping, key)

            def _check_sync():
                try:
                    return self.s3('head_object',
                                   Bucket=s3_mapping['aws_bucket'], Key=s3_key)
                except:
                    return False

            head_resp = wait_for_condition(5, _check_sync)
            self.assertEqual('"%s"' % etag, head_resp['ETag'])

    def test_s3_archive(self):
        s3_mapping = self.s3_archive_mapping()

        test_args = [
            (u'test_archive', u'testing archive put'),
            (u'unicod\u00e9', u'unicod\u00e9 blob')]
        for key, content in test_args:
            s3_key = s3_key_name(s3_mapping, key)
            expected_location = '%s;%s;%s' % (
                s3_mapping['aws_endpoint'],
                s3_mapping['aws_identity'],
                s3_key[:-1 * (len(key) + 1)])

            def etag_func(key):
                hdrs = self.s3(
                    'head_object', Bucket=s3_mapping['aws_bucket'], Key=s3_key)
                return hdrs['ETag'][1:-1]

            self._test_archive(key, content, s3_mapping, etag_func,
                               expected_location)

    def test_swift_sync(self):
        mapping = self.swift_sync_mapping()

        test_args = [
            (u'test_archive', u'testing archive put'),
            (u'unicod\u00e9', u'unicod\u00e9 blob')]
        for key, content in test_args:
            etag = self.local_swift(
                'put_object', mapping['container'], key,
                content.encode('utf-8'))
            self.assertEqual(hashlib.md5(content.encode('utf-8')).hexdigest(),
                             etag)

            def _check_sync():
                try:
                    return self.remote_swift(
                        'head_object', mapping['aws_bucket'], key)
                except:
                    return False

            head_resp = wait_for_condition(5, _check_sync)
            self.assertEqual(etag, head_resp['etag'])

    def test_swift_archive(self):
        mapping = self.swift_archive_mapping()
        expected_location = '%s;%s;%s' % (
                mapping['aws_endpoint'],
                mapping['aws_identity'],
                mapping['aws_bucket'])

        test_args = [
            (u'test_archive', u'testing archive put'),
            (u'unicod\u00e9', u'unicod\u00e9 blob')]

        def get_etag(key):
            hdrs = self.remote_swift('head_object', mapping['aws_bucket'], key)
            return hdrs['etag']

        for key, content in test_args:
            self._test_archive(key, content, mapping, get_etag,
                               expected_location)
