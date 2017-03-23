## 0.1.1 (2017-03-22)

Improvements:

    - Add boto3/botocore logging. This is particularly useful at debug level to
      observe the submitted requests/responses.
    - Added a user agent string for the Google Partner Network.

## 0.1.0 (2017-03-20)

Features:

    - Added SLO support in AWS S3 and Google Cloud Storage. For AWS S3 (and
      clones), SLO is converted to an MPU. Ranges are not supported in SLO
      manifest. If there is a mismatch between the smallest S3 part and Swift,
      i.e. allowing for a segment size < 5MB in Swift, the manifest will fail to
      upload. GCS uploads are converted to a single object, as it has a 5TB
      upload limit.

Improvements:

    - Move s3-sync to using the ContainerCrawler framework.

## 0.0.9 (2016-12-12)

Bug fixes:

    - Fix error handling, where some workers could quit without indicating
      completion of a task, causing the main process to hang.
    - More unicode support fixes.

## 0.0.8 (2016-10-19)

Bug fixes:

    - Properly encode unicode characters in object names and metadata.
    - The `--once` option runs exactly once now.

## 0.0.7 (2016-09-28)

Features:

    - Added support for non-Amazon providers, where we fall back to v2 signer.
    - Glacier integration: objects are re-uploaded if their metadata changes, as
      the metadata is immutable in Glacier.

Bug fixes:

    - Fixed object deletion. Previously, deletes would repeatedly fail and the
      daemon would not make progress.
    - Fixed a bug where `upload_object()` would be called after
      `delete_object()` (even though the object does not exist)

## 0.0.6 (2016-09-05)

Features:

    - Added concurrent uploads, through green threads.

Bug fixes:

    - Avoid extra seeks when attempting to rewind the object which has not been
      read (i.e. calling seek(0) after opening the object).
    - Close the object stream at the end of transfers.

## 0.0.5 (2016-08-16)

Features:

    - Add support for AWS-v4 chunked transfers.

Improvements:

    - Track the database ID and bucket name. If the DB drive crashes and it is
      rebuilt, this will cause the node to re-validate the data already
      uploaded.

    - Exit with status "0" if the config file does not exist. This is important,
      as otherwise a process monitoring system may restart the daemon, on the
      assumption that it encountered an error.

Bug fixes:

    - Configuring the cloud sync daemon for a new bucket resets the sync
      progress.

## 0.0.4 (2016-07-29)

Bug fixes:

    - Account for S3 quoting etags when comparing to the Swift etag (which would
      previously result in repeated uploads).

## 0.0.3 (2016-07-26)

Improvements:

    - Only use the account/container when computing the bucket prefix.
    - Add retry on errors (as opposed to exiting).
    - Early termination if there are no containers to sync.
