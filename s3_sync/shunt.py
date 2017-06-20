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

        sync_profile = next((self.sync_profiles[(acct, c)]
                             for c in (cont, '/*')
                             if (acct, c) in self.sync_profiles), None)
        if sync_profile is None:
            return self.app(env, start_response)

        if not obj and req.method == 'GET':
            return self.handle_listing(req, start_response, sync_profile)
        elif obj and req.method in ('GET', 'HEAD'):
            # TODO: think about what to do for POST, COPY
            return self.handle_object(req, start_response, sync_profile, obj)
        return self.app(env, start_response)

    def handle_container_listing(self, req, start_response, sync_profile):
        status, headers, app_iter = req.call_application(self.app)
        if not status.startswith(('200 ', '204 ')):
            # Only splice 2XX
            start_response(status, headers)
            return app_iter

        provider = create_provider(sync_profile, max_conns=1)  # noqa
        # TODO: do some listings matching the client query, splice

        start_response(status, headers)
        return app_iter

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


def filter_factory(global_conf, **local_conf):
    conf = dict(global_conf, **local_conf)

    def app_filter(app):
        conf_file = conf.get('conf_file', '/etc/swift-s3-sync/sync.json')
        return S3SyncShunt(app, conf_file, conf)
    return app_filter
