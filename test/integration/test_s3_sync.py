"""
Copyright 2017 SwiftStack

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import boto3
import botocore.exceptions
import hashlib
import json
import os
import StringIO
import subprocess
import swiftclient
import time
import unittest


def clear_swift_container(client, container):
    _, list_results = client.get_container(container)
    for obj in list_results:
        client.delete_object(container, obj['name'])


def clear_s3_bucket(client, bucket):
    list_results = client.list_objects(Bucket=bucket)
    for obj in list_results.get('Contents', []):
        client.delete_object(Bucket=bucket, Key=obj['Key'])


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
    return hex(long(md5_prefix.hexdigest(), 16) % 16 ** 6)[2:-1]


def s3_key_name(mapping, key):
    prefix = s3_prefix(
        mapping['account'],
        mapping['container'],
        key)
    return '%s/%s/%s/%s' % (
        prefix, mapping['account'], mapping['container'], key)


def swift_content_location(mapping):
    return '%s;%s;%s' % (mapping['aws_endpoint'],
                         mapping['aws_identity'],
                         mapping['aws_bucket'])


def get_container_ports(image_name):
    if 'DOCKER' in os.environ:
        return dict(swift=8080, s3=10080)
    if 'TEST_CONTAINER' in os.environ:
        container = os.environ['TEST_CONTAINER']
    else:
        cmd = 'docker ps -f ancestor=%s -f status=running '\
              '--format "{{.Names}}"' % image_name
        images = subprocess.check_output(cmd.split())
        if not images:
            raise RuntimeError('Cannot find container from image %s' %
                               image_name)
        container = images.split()[0][1:-1]

    cmd = 'docker port %s' % container
    try:
        ports = {}
        for line in subprocess.check_output(cmd.split()).split('\n'):
            if not line.strip():
                continue
            docker, host = line.split(' -> ')
            docker_port = int(docker.split('/')[0])
            host_port = int(host.split(':')[1])
            if docker_port == 8080:
                ports['swift'] = host_port
            elif docker_port == 10080:
                ports['s3'] = host_port
    except subprocess.CalledProcessError as e:
        print e.output
        print e.retcode
        raise
    return ports


class TestCloudSync(unittest.TestCase):
    IMAGE_NAME = 'swift-s3-sync'
    PORTS = get_container_ports(IMAGE_NAME)

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
                    clear_s3_bucket(self.s3_client, container['aws_bucket'])
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
    def s3_restore_mapping(klass):
        return klass._find_mapping(
            lambda cont:
                cont['protocol'] == 's3' and cont.get('restore_object', False))

    @classmethod
    def swift_restore_mapping(klass):
        return klass._find_mapping(
            lambda cont:
                cont['protocol'] == 'swift' and
                cont.get('restore_object', False))

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
                except Exception:
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
                except Exception:
                    return False

            head_resp = wait_for_condition(5, _check_sync)
            self.assertEqual(etag, head_resp['etag'])

    def test_swift_archive(self):
        mapping = self.swift_archive_mapping()
        expected_location = swift_content_location(mapping)

        test_args = [
            (u'test_archive', u'testing archive put'),
            (u'unicod\u00e9', u'unicod\u00e9 blob')]

        def get_etag(key):
            hdrs = self.remote_swift('head_object', mapping['aws_bucket'], key)
            return hdrs['etag']

        for key, content in test_args:
            self._test_archive(key, content, mapping, get_etag,
                               expected_location)

    def test_s3_archive_get(self):
        tests = [{'content': 's3 archive and get',
                  'key': 'test_s3_archive'},
                 {'content': '',
                  'key': 'test-empty'}]

        for test in tests:
            content = test['content']
            key = test['key']
            s3_mapping = self.s3_restore_mapping()
            s3_key = s3_key_name(s3_mapping, key)
            self.s3('put_object',
                    Bucket=s3_mapping['aws_bucket'],
                    Key=s3_key,
                    Body=StringIO.StringIO(content))

            hdrs = self.local_swift(
                'head_object', s3_mapping['container'], key)
            self.assertIn('server', hdrs)
            self.assertTrue(hdrs['server'].startswith('Jetty'))

            hdrs, body = self.local_swift(
                'get_object', s3_mapping['container'], key, content)
            self.assertEqual(hashlib.md5(content).hexdigest(), hdrs['etag'])
            swift_content = ''.join([chunk for chunk in body])
            self.assertEqual(content, swift_content)
            # There should be a "server" header, set to Jetty for S3Proxy
            self.assertEqual('Jetty(9.2.z-SNAPSHOT)', hdrs['server'])

            # the subsequent request should come back from Swift
            hdrs, body = self.local_swift(
                'get_object', s3_mapping['container'], key)
            swift_content = ''.join([chunk for chunk in body])
            self.assertEqual(content, swift_content)
            self.assertEqual(False, 'server' in hdrs)
        clear_s3_bucket(self.s3_client, s3_mapping['aws_bucket'])
        clear_swift_container(self.swift_src, s3_mapping['container'])

    def test_s3_archive_slo_restore(self):
        # Satisfy the 5MB minimum MPU part size
        content = 'A' * (6 * 1024 * 1024)
        key = 'test_swift_archive'
        mapping = self.s3_restore_mapping()
        s3_key = s3_key_name(mapping, key)
        manifest_key = s3_key_name
        prefix, account, container, _ = s3_key.split('/', 3)
        key_hash = hashlib.sha256(key).hexdigest()
        manifest_key = '/'.join([
            prefix, '.manifests', account, container,
            '%s.swift_slo_manifest' % (key_hash)])
        manifest = [
            {'bytes': 5 * 1024 * 1024, 'name': '/segments/part1'},
            {'bytes': 1024 * 1024, 'name': '/segments/part2'}]
        self.s3('put_object',
                Bucket=mapping['aws_bucket'],
                Key=manifest_key,
                Body=json.dumps(manifest))
        resp = self.s3('create_multipart_upload',
                       Bucket=mapping['aws_bucket'],
                       Key=s3_key,
                       Metadata={'x-static-large-object': 'True'})
        self.s3('upload_part',
                Bucket=mapping['aws_bucket'],
                Key=s3_key,
                PartNumber=1,
                UploadId=resp['UploadId'],
                Body=content[:(5 * 1024 * 1024)])
        self.s3('upload_part',
                Bucket=mapping['aws_bucket'],
                Key=s3_key,
                PartNumber=2,
                UploadId=resp['UploadId'],
                Body=content[(5 * 1024 * 1024):])
        self.s3('complete_multipart_upload',
                Bucket=mapping['aws_bucket'],
                Key=s3_key,
                UploadId=resp['UploadId'],
                MultipartUpload={
                    'Parts': [
                        {'PartNumber': 1,
                         'ETag': hashlib.md5(
                             content[:(5 * 1024 * 1024)]).hexdigest()},
                        {'PartNumber': 2,
                         'ETag': hashlib.md5(
                             content[(5 * 1024 * 1024):]).hexdigest()}]})

        hdrs, listing = self.local_swift('get_container', mapping['container'])
        self.assertEqual(0, int(hdrs['x-container-object-count']))
        for entry in listing:
            self.assertIn('content_location', entry)

        hdrs, body = self.local_swift(
            'get_object', mapping['container'], key, content)
        # NOTE: this is different from real S3 as all of the parts are merged
        # and this is the content ETag
        self.assertEqual(hashlib.md5(content).hexdigest(), hdrs['etag'])
        swift_content = ''.join([chunk for chunk in body])
        self.assertEqual(content, swift_content)
        self.assertEqual('True', hdrs['x-static-large-object'])

        # the subsequent request should come back from Swift
        hdrs, listing = self.local_swift('get_container', mapping['container'])
        self.assertEqual(1, int(hdrs['x-container-object-count']))
        # We get back an entry for the remote and the local object
        self.assertIn('content_location', listing[0])
        self.assertFalse('content_location' in listing[1])
        hdrs, body = self.local_swift('get_object', mapping['container'], key)
        swift_content = ''.join([chunk for chunk in body])
        self.assertEqual(content, swift_content)

        for k in hdrs.keys():
            self.assertEqual(False, k.startswith('Remote-'))
        clear_s3_bucket(self.s3_client, mapping['aws_bucket'])
        clear_swift_container(self.swift_src, mapping['container'])
        clear_swift_container(self.swift_src, 'segments')

    def test_swift_archive_get(self):
        content = 'swift archive and get'
        key = 'test_swift_archive'
        mapping = self.swift_restore_mapping()
        self.remote_swift('put_object', mapping['aws_bucket'], key, content)

        hdrs, listing = self.local_swift('get_container', mapping['container'])
        self.assertEqual(0, int(hdrs['x-container-object-count']))
        for entry in listing:
            self.assertIn('content_location', entry)
            self.assertEqual(swift_content_location(mapping),
                             entry['content_location'])

        hdrs, body = self.local_swift(
            'get_object', mapping['container'], key, content)
        self.assertEqual(hashlib.md5(content).hexdigest(), hdrs['etag'])
        swift_content = ''.join([chunk for chunk in body])
        self.assertEqual(content, swift_content)

        # the subsequent request should come back from Swift
        hdrs, listing = self.local_swift('get_container', mapping['container'])
        self.assertEqual(1, int(hdrs['x-container-object-count']))
        # We get back an entry for the remote and the local object
        self.assertIn('content_location', listing[0])
        self.assertEqual(swift_content_location(mapping),
                         listing[0]['content_location'])
        self.assertFalse('content_location' in listing[1])
        hdrs, body = self.local_swift('get_object', mapping['container'], key)
        swift_content = ''.join([chunk for chunk in body])
        self.assertEqual(content, swift_content)
        clear_swift_container(self.swift_dst, mapping['aws_bucket'])
        clear_swift_container(self.swift_src, mapping['container'])

    def test_swift_archive_slo_restore(self):
        content = 'A' * 2048
        key = 'test_swift_archive'
        mapping = self.swift_restore_mapping()
        manifest = [
            {'size_bytes': 1024, 'path': '/segments/part1'},
            {'size_bytes': 1024, 'path': '/segments/part2'}]
        self.remote_swift('put_container', 'segments')
        self.remote_swift('put_object', 'segments', 'part1', content[:1024])
        self.remote_swift('put_object', 'segments', 'part2', content[1024:])
        self.remote_swift('put_object', mapping['aws_bucket'], key,
                          json.dumps(manifest),
                          query_string='multipart-manifest=put')

        hdrs, listing = self.local_swift('get_container', mapping['container'])
        self.assertEqual(0, int(hdrs['x-container-object-count']))
        for entry in listing:
            self.assertIn('content_location', entry)
            self.assertEqual(swift_content_location(mapping),
                             entry['content_location'])

        slo_etag = hashlib.md5(''.join([
            hashlib.md5(content[:1024]).hexdigest(),
            hashlib.md5(content[1024:]).hexdigest()])).hexdigest()
        hdrs, body = self.local_swift(
            'get_object', mapping['container'], key, content)
        self.assertEqual('"%s"' % slo_etag, hdrs['etag'])
        swift_content = ''.join([chunk for chunk in body])
        self.assertEqual(content, swift_content)
        self.assertEqual('True', hdrs['x-static-large-object'])

        # the subsequent request should come back from Swift
        hdrs, listing = self.local_swift('get_container', mapping['container'])
        self.assertEqual(1, int(hdrs['x-container-object-count']))
        # We get back an entry for the remote and the local object
        self.assertIn('content_location', listing[0])
        self.assertEqual(swift_content_location(mapping),
                         listing[0]['content_location'])
        self.assertFalse('content_location' in listing[1])
        hdrs, body = self.local_swift('get_object', mapping['container'], key)
        swift_content = ''.join([chunk for chunk in body])
        self.assertEqual(content, swift_content)

        for k in hdrs.keys():
            self.assertEqual(False, k.startswith('Remote-'))
        clear_swift_container(self.swift_dst, mapping['aws_bucket'])
        clear_swift_container(self.swift_dst, 'segments')
        clear_swift_container(self.swift_src, mapping['container'])
        clear_swift_container(self.swift_src, 'segments')
