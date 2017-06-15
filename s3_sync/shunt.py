import json

from swift.common import constraints, swob, utils

from swiftstack_auth.utils import fix_mw_logging

from .provider_factory import create_provider


class S3SyncShunt(object):
    def __init__(self, app, conf_file, conf):
        self.app = app
        with open(conf_file, 'rb') as fp:
            conf = json.load(fp)
        self.logger = fix_mw_logging(utils.get_logger(
            conf, name='proxy-server:s3_sync.shunt',
            log_route='s3_sync.shunt'))
        self.sync_profiles = {}
        for cont in conf['containers']:
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

        provider = create_provider(sync_profile, max_conns=1)  # noqa
        # TODO: actually shunt requests

        start_response(status, headers)
        return app_iter


def filter_factory(global_conf, **local_conf):
    conf = dict(global_conf, **local_conf)

    def app_filter(app):
        conf_file = conf.get('conf_file', '/etc/swift-s3-sync/sync.json')
        return S3SyncShunt(app, conf_file, conf)
    return app_filter
