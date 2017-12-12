Swift-S3 Sync
-------------

Swift-S3 Sync is a way to share data between on-premises [OpenStack
Swift](https://github.com/openstack/swift)
deployments and [Amazon S3](https://aws.amazon.com/s3) (or S3-clones). The
project initially allowed for propagating any changes from Swift to S3 -- PUT,
DELETE, or POST -- in an asynchronous fashion. Since then, it has evolved to
support a limited set of data policy options to express the lifecycle of the
data and transparent access to data stored in S3.

Notable features:

- asynchronously propagates object operations to Amazon S3, Google Cloud
  Storage&#185;, S3-clones, and other Swift Clusters
- allows for an "archival" mode after set time period
- on-line access to archived data through the Swift interface

&#185;Google Cloud Storage requires [interoperability
access](https://cloud.google.com/storage/docs/migrating#keys) to be enabled.

### Design overview

`swift-s3-sync` runs as a standalone process, intended to be used on Swift
container nodes. The container database provides the list of changes to the
objects in Swift (whether it was a metadata update, new object, or a deletion).

To provide on-line access to archived Swift objects, there is a Swift middleware
[component](https://github.com/swiftstack/swift-s3-sync/blob/master/s3_sync/shunt.py). If a
Swift container was configured to be archived, the middleware will query the
destination store for contents on a GET request, as well as splice the results
of LIST requests between the two stores.

There is no explicit coordination between the `swift-s3-sync` daemons.
Implicitly, they coordinate through their progress in the container database.
Each daemon looks up the number of container nodes in the system (with the
assumption that each node has a running daemon). Initially, each only handles
the objects assigned to it. Afterward, each one verifies that the other objects
have been processed, as well. This means that for each operation, there are
as many requests issued against the remote store as there are container
databases for the container. For example, in a three replica policy, there would
be three HEAD requests if an object PUT was performed (but only one PUT against
the remote store in the common case).

### How to setup and use

`swift-s3-sync` depends on:

- [container-crawler library](https://github.com/swiftstack/container-crawler)
- [botocore](https://github.com/swiftstack/botocore/tree/1.4.32.5)
  (unfortunately, we had to use our own fork, as a number of patches were
  difficult to merge upstream)
- [boto](https://github.com/boto/boto3/tree/1.3.1)
- [eventlet](https://github.com/eventlet/eventlet)

Until we can merge the boto patches, you will also have to install botocore from
our fork (do this before installing swift-s3-sync):
`pip install -e git://github.com/swiftstack/botocore.git@1.4.32.5#egg=botocore`

Build the package to be installed on the nodes with:
```
python ./setup.py build sdist
```

Install the tarball with:
```
pip install swift-s3-sync-<version>.tar.gz
```

You also will need to install the `container-crawler` library from Git:
```
pip install -e git://github.com/swiftstack/container-crawler.git@0.0.9#egg=container-crawler
```

After that, you should have the `swift-s3-sync` executable available in
`/usr/local/bin`.

`swift-s3-sync` has to be invoked with a configuration file, specifying which
containers to watch, where the contents should be placed, as well as a number of
global settings. A sample configuration file is in the
[repository](https://github.com/swiftstack/swift-s3-sync/blob/master/sync.json-sample).

To configure the Swift Proxy servers to use `swift-s3-sync` to redirect requests
for archived objects, you have to add the following to the proxy pipeline:
```
[filter:swift_s3_shunt]
use = egg:swift-s3-sync#cloud-shunt
conf_file = <Path to swift-s3-sync config file>
```

This middleware should be in the pipeline before the DLO/SLO middleware.

### Trying it out

To try this out, you can build the docker container used for integration tests:
`docker build -t swift-s3-sync test/container`. It uses S3Proxy, backed by the
filesystem and uses a Swift all-in-one docker container as the base.

The container must map the swift-s3-sync source tree when launching:

```
docker run -P -d -v `pwd`:/swift-s3-sync swift-s3-sync
```

After this, we can check the port mappings: `docker port <container name>`.

Port 8080 is the Swift Proxy server, whereas 10080 is the S3Proxy.

Tests pre-configure multiple
[policies](https://github.com/swiftstack/swift-s3-sync/blob/master/test/container/swift-s3-sync.conf).

Specifically, you can create containers `sync-s3` and `archive-s3` to observe
how swift-s3-sync works. Using `python-swiftclient`, that would look something
like this:

```
swift -A http://localhost:<mapped 8080>/auth/v1.0 -U test:tester -K testing post sync-s3
swift -A http://localhost:<mapped 8080>/auth/v1.0 -U test:tester -K testing post archive-s3
swift -A http://localhost:<mapped 8080>/auth/v1.0 -U test:tester -K testing put sync-s3 <file>
swift -A http://localhost:<mapped 8080>/auth/v1.0 -U test:tester -K testing put archive-s3 <file>
```

After this, we can examine the state of our fake S3 content:

```
s3cmd ls -r s3://s3-sync-stest/
```

You should see two objects in the bucket. For s3cmd, you can use the following
configuration:

```
access_key=s3-sync-test
secret_key=s3-sync-test
host_base=localhost:<mapped 10080>
host_bucket=localhost:<mapped 10080>
```

### Running tests


#### Unit tests

All commands below assume you're running them in the swift-s3-sync directory.

It is recommended to setup virtualenv when working on this project. You can set
it up as follows: `virtualenv venv`. After that, dependencies can be setup with:
`./venv/bin/pip install -r requirements.txt`. The test dependencies can be
similarly setup through:
`./venv/bin/pip install -r requirements-test.txt`.

To run unit tests, you can simply run `nose`: `./venv/bin/nosetests`. The unit
tests will require swift and container-crawler to be in your
`PYTHONPATH`. Typically, I run them with the following convoluted line:
`PYTHONPATH=~/swift:~/container-crawler ./venv/bin/nosetests`.
`PYTHONPATH=~/swift:~/container-crawler ./venv/bin/nosetests`.
This assumes that all of the dependencies are in your home directory. You can
adjust this if they live in other places.

#### Integration tests

For integration tests, we need access to a Swift cluster and some sort of an S3
provider. Currently, the tests use a Docker container to provide Swift and are
configured to talk to [S3Proxy](https://github.com/andrewgaul/s3proxy).

To build the test container, run:
`docker build -t swift-s3-sync test/container`

Once this completes, you will have a docker container tagged `swift-s3-sync`.
Start the container with:

`docker run -P -d -v <swift-s3-sync checkout>:/swift-s3-sync swift-s3-sync`.
The container will be started in the background (`-d`) and will expose ports
8080 and 10080 (`-P`) to connect to Swift and S3Proxy, respectively. It is based
on the
[bouncestorage/swift-aio](https://hub.docker.com/r/bouncestorage/swift-aio/).
The `-v` option maps the current source tree into the container, so that it
operates on your current state.

NOTE: the services do not restart on code changes. You can either manually
stop/start the swift-s3-sync daemon (and Swift proxy if you're working on the
shunt), or stop/start the contianer.

The cloud sync configuration for the tests is defined in
`test/container/swift-s3-sync.conf`. In particular, there are mappings for S3
sync and archive policies and the same for Swift. The S3 mappings point to
S3Proxy running on the host machine, listening on port 10080.

Once you have S3Proxy and the Docker container running, run the tests with:
```
./venv/bin/nosetests test/integration
```

By default tests will look for the first running container that has been started
from an image named `swift-s3-sync`. You can override that behavior by
specifying the test container to use with the `TEST_CONTAINER` environment
variable.

The tests create and destroy the containers and buckets configured in the
`swift-s3-sync.conf` file. If you need to examine the state of a container,
consider commenting out the `tearDownClass` method to be a NOOP (TODO: add a way
to keep state).

If you would like to examine the logs from each of the services, all logs are in
/var/log (e.g. /var/log/swift-s3-sync.log).
