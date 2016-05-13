from swift.common.daemon import run_daemon
from swift.common.utils import parse_options
import s3_sync

conf_file, options = parse_options(once=True)
run_daemon(s3_sync.S3Sync, conf_file, **options)
