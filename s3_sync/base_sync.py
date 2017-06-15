import eventlet
import logging


class BaseSync(object):
    """Generic base class that each provider must implement.

       These classes implement the actual data transfers, validation that
       objects have been propagated, and any other related operations to
       propagate Swift objects and metadata to a remote endpoint.
    """

    HTTP_CONN_POOL_SIZE = 1
    SLO_WORKERS = 10
    SLO_QUEUE_SIZE = 100
    MB = 1024*1024
    GB = 1024*MB

    class HttpClientPoolEntry(object):
        def __init__(self, client, pool):
            self.semaphore = eventlet.semaphore.Semaphore(
                BaseSync.HTTP_CONN_POOL_SIZE)
            self.client = client
            self.pool = pool

        def acquire(self):
            return self.semaphore.acquire(blocking=False)

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            self.semaphore.release()
            self.pool.release()

    class HttpClientPool(object):
        def __init__(self, client_factory, max_conns):
            self.get_semaphore = eventlet.semaphore.Semaphore(max_conns)
            self.client_pool = self._create_pool(client_factory, max_conns)

        def _create_pool(self, client_factory, max_conns):
            clients = max_conns / BaseSync.HTTP_CONN_POOL_SIZE
            if max_conns % BaseSync.HTTP_CONN_POOL_SIZE:
                clients += 1
            return [BaseSync.HttpClientPoolEntry(client_factory(), self)
                    for _ in range(0, clients)]

        def get_client(self):
            # SLO uploads may exhaust the client pool and we will need to wait
            # for connections
            self.get_semaphore.acquire()
            # we are guaranteed that there is an open connection we can use
            for client in self.client_pool:
                if client.acquire():
                    return client
            raise RuntimeError('Pool was exhausted')  # should never happen

        def release(self):
            self.get_semaphore.release()

    def __init__(self, settings, max_conns=10):
        self.settings = settings
        self.account = settings['account']
        self.container = settings['container']
        self.logger = logging.getLogger('s3-sync')

        # Due to the genesis of this project, the endpoint and bucket have the
        # "aws_" prefix, even though the endpoint may actually be a Swift
        # cluster and the "bucket" is a container.
        self.endpoint = settings.get('aws_endpoint', None)
        self.aws_bucket = settings['aws_bucket']

        self.client_pool = self.HttpClientPool(
            self._get_client_factory(), max_conns)

    def __repr__(self):
        return '<%s: %s/%s>' % (
            self.__class__.__name__,
            's3:/' if self.endpoint is None else self.endpoint.rstrip('/'),
            self.aws_bucket,
        )

    def upload_object(self, name, storage_policy_index):
        raise NotImplementedError()

    def delete_object(self, name):
        raise NotImplementedError()

    def _get_client_factory(self):
        raise NotImplementedError()

    def _full_name(self, key):
        return u'%s/%s/%s' % (self.account, self.container,
                              key.decode('utf-8'))
