from .sync_s3 import SyncS3
from .sync_swift import SyncSwift


def create_provider(sync_settings, max_conns, per_account=False):
    provider_type = sync_settings.get('protocol', None)
    if not provider_type or provider_type == 's3':
        return SyncS3(sync_settings, max_conns, per_account)
    elif provider_type == 'swift':
        return SyncSwift(sync_settings, max_conns, per_account)
    else:
        raise NotImplementedError()
