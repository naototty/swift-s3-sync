## 0.1.9 (2017-06-21)

Bug fixes:

    - The shunt should propagate errors encountered from S3 (e.g. 404) to the
      client, as opposed to always returning 502.

## 0.1.8 (2017-06-21)

Bug fixes:

    - When syncing *all* containers in an account, the middleware needs to use
      the requested container when looking up the object in S3.

## 0.1.7 (2017-06-20)

Features:

    - When uploading data to Amazon S3, AES256 Server-Side encryption will be
      used by default.
    - Added middleware to allow for LIST and GET of objects that may have been
      archived to the remote bucket.

Bug fixes:

    - Supply content-length with Swift objects on PUT. This ensures that we can
      upload a 0-sized object.
    - Fixed Swift DELETE propagation. Previously, DELETE requests would fail due
      to a missing parameter.

Known issues:

    - Sync all containers is currently not working as intended with Swift. It
      places all of the objects in one container. Will address in a subsequent
      release.

## 0.1.6 (2017-06-02)

Bug fixes:

    - Fix an issue that prevents SLO uploads where opening a Swift connection
      before acquiring the S3 client may cause the Swift connection to be closed
      before any bytes are read.
    - Do not serialize on a single Boto session.

## 0.1.5 (2017-06-01)

Bug fixes:

    - Handle deleted objects when DELETE propagation is turned off correctly
      (should be a NOOP, but previously fell through to an attempted upload).
    - Handle "409 Conflict" if attempting to DELETE an object, but it was
      actually already replaced with a new Timestamp.

## 0.1.4 (2017-05-30)

Features:

    - Allow fine(r) grained control of object movement through `copy_after`,
      `retain_local`, and `propagate_delete` options. `copy_after` defers action
      on the rows until after a specified number of seconds has expired since
      the last object update; `retain_local` determines whether the object
      should be removed after copying to the remote store; `propagate_delete`
      controls whether DELETE requests against the cluster should show up on the
      remote endpoint. For example, one could configure Cloud Sync in archive
      mode, by turning off DELETE propagation and local copy retention, while
      defering the copy action for a set number of days until the archival date.

Bug fixes:

    - A missing object should not generate an exception -- and stop Cloud Sync
      -- when attempting to upload. The exception will now be ignored.

## 0.1.3 (2017-05-08)

Improvement:

    - Support new version of the ContainerCrawler (0.0.3).

## 0.1.2 (2017-04-19)

Features:

    - Implemented support for syncing to Swift. Does not support DLO, but does
      have parity with S3 sync (propagates PUT, POST, DELETE, and supports
      SLOs). Swift can be enabled by passing the option "protocol" with the
      value "swift" in the configuration for a mapping.

Bug fixes:

    - Fixed a broken import, which prevented the daemon from starting.
    - Restricted the requests Sessions to be used once per worker (as opposed to
      being shared across workers).

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
