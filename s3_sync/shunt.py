from lxml import etree
import json

from swift.common import constraints, swob, utils

from swiftstack_auth.utils import fix_mw_logging

from .provider_factory import create_provider


class S3SyncShunt(object):
    def __init__(self, app, conf_file, conf):
        self.logger = fix_mw_logging(utils.get_logger(
            conf, name='proxy-server:s3_sync.shunt',
            log_route='s3_sync.shunt'))

        self.app = app
        try:
            with open(conf_file, 'rb') as fp:
                conf = json.load(fp)
        except (IOError, ValueError) as err:
            self.logger.warning("Couldn't read conf_file %r: %s; disabling",
                                conf_file, err)
            conf = {'containers': []}

        self.sync_profiles = {}
        for cont in conf['containers']:
            for key in ('account', 'container'):
                cont[key] = cont[key].encode('utf8')
            if cont.get('propagate_delete', True):
                # object shouldn't exist in remote
                continue
            self.sync_profiles[(cont['account'], cont['container'])] = cont

    def __call__(self, env, start_response):
        req = swob.Request(env)
        try:
            vers, acct, cont, obj = req.split_path(3, 4, True)
        except ValueError:
            return self.app(env, start_response)

        if not constraints.valid_api_version(vers):
            return self.app(env, start_response)

        if not cont:
            return self.app(env, start_response)

        sync_profile = next((self.sync_profiles[(acct, c)]
                             for c in (cont, '/*')
                             if (acct, c) in self.sync_profiles), None)
        if sync_profile is None:
            return self.app(env, start_response)
        # If mapping all containers, make a new profile for just this request
        if sync_profile['container'] == '/*':
            sync_profile = dict(sync_profile, container=cont)

        if not obj and req.method == 'GET':
            return self.handle_listing(req, start_response, sync_profile, cont)
        elif obj and req.method in ('GET', 'HEAD'):
            # TODO: think about what to do for POST, COPY
            return self.handle_object(req, start_response, sync_profile, obj)
        return self.app(env, start_response)

    def handle_listing(self, req, start_response, sync_profile, cont):
        limit = int(req.params.get(
            'limit', constraints.CONTAINER_LISTING_LIMIT))
        marker = req.params.get('marker', '')
        prefix = req.params.get('prefix', '')
        delimiter = req.params.get('delimiter', '')
        path = req.params.get('path', None)
        if path:
            # We do not support the path parameter in listings
            status, headers, app_iter = req.call_application(self.app)
            start_response(status, headers)
            return app_iter

        req_format = req.params.get('format', None)
        # We always make the request with the json format and convert to the
        # client-expected response.
        req.params = dict(req.params, format='json')
        status, headers, app_iter = req.call_application(self.app)
        if not status.startswith(('200 ', '204 ')):
            # Only splice 2XX
            start_response(status, headers)
            return app_iter

        provider = create_provider(sync_profile, max_conns=1)  # noqa
        cloud_status, resp = provider.list_objects(
            marker, limit, prefix, delimiter)
        if cloud_status != 200:
            self.logger.error('Failed to list the remote store: %s' % resp)
            resp = []
        if not resp:
            start_response(status, headers)
            return app_iter

        internal_resp = json.load(utils.FileLikeIter(app_iter))
        spliced_response = []
        internal_index = 0
        cloud_index = 0
        while True:
            if len(spliced_response) == limit:
                break

            if len(resp) == cloud_index and \
                    len(internal_resp) == internal_index:
                break

            if internal_index < len(internal_resp):
                if 'name' in internal_resp[internal_index]:
                    internal_name = internal_resp[internal_index]['name']
                elif 'subdir' in internal_resp[internal_index]:
                    internal_name = internal_resp[internal_index]['subdir']
            else:
                internal_name = None

            if cloud_index < len(resp):
                if 'name' in resp[cloud_index]:
                    cloud_name = resp[cloud_index]['name']
                else:
                    cloud_name = resp[cloud_index]['subdir']
            else:
                cloud_name = None

            if cloud_name is not None:
                if internal_name is None or \
                        (internal_name is not None and \
                         cloud_name <= internal_name):
                    spliced_response.append(resp[cloud_index])
                    cloud_index += 1
                    if len(resp) == cloud_index:
                        cloud_status, resp = provider.list_objects(
                            cloud_name, limit, prefix, delimiter)
                        if cloud_status != 200:
                            self.logger.error(
                                'Failed to list the remote store: %s' % resp)
                            resp = []
                        cloud_index = 0
                    continue
            spliced_response.append(internal_resp[internal_index])
            internal_index += 1

        res = self._format_listing_response(spliced_response, req_format, cont)
        dict_headers = dict(headers)
        dict_headers['Content-Length'] = len(res)
        if req_format == 'json':
            dict_headers['Content-Type'] = 'application/json'
        elif req_format == 'xml':
            dict_headers['Content-Type'] = 'application/xml'
        else:
            dict_headers['Content-Type'] = 'text/plain'
        start_response(status, dict_headers.items())
        return res

    def handle_object(self, req, start_response, sync_profile, obj):
        status, headers, app_iter = req.call_application(self.app)
        if not status.startswith('404 '):
            # Only shunt 404s
            start_response(status, headers)
            return app_iter
        self.logger.debug('404 for %s; shunting to %r'
                          % (req.path, sync_profile))

        # Save off any existing trans-id headers so we can add them back later
        trans_id_headers = [(h, v) for h, v in headers if h.lower() in (
            'x-trans-id', 'x-openstack-request-id')]

        utils.close_if_possible(app_iter)

        provider = create_provider(sync_profile, max_conns=1)
        status_code, headers, app_iter = provider.shunt_object(req, obj)
        status = '%s %s' % (status_code, swob.RESPONSE_REASONS[status_code][0])
        self.logger.debug('Remote resp: %s' % status)

        # Blacklist of known hop-by-hop headers taken from
        # https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers
        headers_to_remove = set([
            'connection',
            'keep-alive',
            'proxy-authenticate',
            'proxy-authorization',
            'te',
            'trailer',
            'transfer-encoding',
            'upgrade',
        ])
        indexes_to_remove = [
            i for i, (header, key) in enumerate(headers)
            if header.lower() in headers_to_remove]
        headers = [item for i, item in enumerate(headers)
                   if i not in indexes_to_remove]
        headers.extend(trans_id_headers)

        start_response(status, headers)
        return app_iter

    @staticmethod
    def _format_listing_response(list_results, list_format, container):
        if list_format == 'json':
            return json.dumps(list_results)
        if list_format == 'xml':
            fields = ['name', 'content_type', 'hash', 'bytes', 'last_modified',
                      'subdir']
            root = etree.Element('container', name=container)
            for entry in list_results:
                obj = etree.Element('object')
                for f in fields:
                    if f not in entry:
                        continue
                    el = etree.Element(f)
                    text = entry[f]
                    if type(text) == str:
                        text = text.decode('utf-8')
                    elif type(text) == int:
                        text = str(text)
                    el.text = text
                    obj.append(el)
                root.append(obj)
            resp = etree.tostring(root, encoding='UTF-8', xml_declaration=True)
            return resp.replace("<?xml version='1.0' encoding='UTF-8'?>",
                                '<?xml version="1.0" encoding="UTF-8"?>', 1)

        # Default to plain format
        return '\n'.join([entry['name'] for entry in list_results])


def filter_factory(global_conf, **local_conf):
    conf = dict(global_conf, **local_conf)

    def app_filter(app):
        conf_file = conf.get('conf_file', '/etc/swift-s3-sync/sync.json')
        return S3SyncShunt(app, conf_file, conf)
    return app_filter
