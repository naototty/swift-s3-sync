"""
Copyright 2018 SwiftStack

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

import argparse
from cStringIO import StringIO
import hashlib
import urlparse

import botocore.exceptions
import swiftclient.exceptions
from .provider_factory import create_provider


class FakeInternalClient(object):
    def __init__(self, data, metadata):
        self.data = data
        self.metadata = metadata
        self.metadata.update({
            'etag': hashlib.md5(data).hexdigest(),
            'Content-Length': str(len(data)),
            'content-type': 'text/plain',
        })

    def get_object_metadata(self, acct, cont, obj, headers=None):
        return self.metadata

    def get_object(self, acct, cont, obj, headers=None):
        # NB: Need StringIO so we have a close() method; need it every call
        # so retries don't try to read from a closed reader
        return 200, self.metadata, StringIO(self.data)


def validate_bucket(provider, swift_key, create_bucket):
    if create_bucket:
        # This should only be necessary on Swift; reach down to the client
        with provider.client_pool.get_client() as client:
            result = client.put_container(provider.aws_bucket)
        if result is not None:
            return result

    internal_client = FakeInternalClient('cloud sync test', {})
    result = provider.upload_object(swift_key, 0, internal_client)
    if result is not None:
        return result

    result = provider.update_metadata(swift_key, {
        'X-Object-Meta-Cloud-Sync': 'fabcab',
        'content-type': 'text/plain'})
    if result is not None:
        return result

    result = provider.head_object(swift_key)
    if result.status != 200:
        return 'Unexpected status code checking write: %s' % result.status
    if result.headers['x-object-meta-cloud-sync'] != 'fabcab':
        return 'Unexpected headers after setting metadata: %s' % result.headers

    result = provider.delete_object(swift_key)
    if result is not None:
        return result

    status, objects = provider.list_objects(
        marker='', limit=1, prefix='', delimiter='')
    if status != 200:
        return 'Unexpected status code listing bucket: %s' % status

    if create_bucket:
        # Clean up after ourselves
        with provider.client_pool.get_client() as client:
            result = client.delete_container(provider.aws_bucket)
        if result is not None:
            return result


def main(args=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--protocol', required=True, choices=('s3', 'swift'))
    parser.add_argument('--endpoint', required=True)
    parser.add_argument('--username', required=True)
    parser.add_argument('--password', required=True)
    parser.add_argument('--account')
    parser.add_argument('--bucket')
    args = parser.parse_args(args)
    conf = {
        'protocol': args.protocol,
        'account': 'verify-auth',
        'container': u'testing-\U0001f44d',
        'aws_endpoint': args.endpoint,
        'aws_identity': args.username,
        'aws_secret': args.password,
        'remote_account': args.account,
        'aws_bucket': args.bucket,
    }
    if args.account and args.protocol != 'swift':
        return 'Invalid argument: account is only valid with swift protocol'
    if args.bucket == '/*':
        conf['aws_bucket'] = u'.cloudsync_test_container-\U0001f44d'
    if urlparse.urlparse(args.endpoint).hostname.endswith('.amazonaws.com'):
        conf['aws_endpoint'] = None  # let Boto sort it out

    if conf['aws_bucket'] and '/' in conf['aws_bucket']:
        return 'Invalid argument: slash is not allowed in container name'

    provider = create_provider(conf, max_conns=1)
    try:
        if not args.bucket:
            with provider.client_pool.get_client() as client:
                if args.protocol == 's3':
                    client.list_buckets()
                else:
                    client.get_account()
        else:
            if args.protocol == 's3':
                swift_key = 'fabcab/cloud_sync_test'
            else:
                swift_key = 'cloud_sync_test_object'
            result = validate_bucket(
                provider, swift_key,
                args.protocol == 'swift' and args.bucket == '/*')
            if result is not None:
                return result
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] in ('SignatureDoesNotMatch', '403'):
            return ('Invalid credentials. Please check the Access Key '
                    'ID and Secret Access Key.')
        else:
            return 'Unexpected error validating credentials: %r' % (
                e.response['Error']['Message'],)
    except swiftclient.exceptions.ClientException as e:
        if e.http_status == 401:
            return ('Invalid credentials. Please check the Access Key '
                    'ID and Secret Access Key.')
        elif e.http_status == 403:
            return ('Unauthorized. Please check that you have access to '
                    'the account and/or container.')
        else:
            return 'Unexpected error validating credentials: %s' % (e,)
    return 0


if __name__ == '__main__':
    exit(main())
