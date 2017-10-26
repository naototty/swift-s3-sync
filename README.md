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
`docker build -t cloud-sync/test test/container`

Once this completes, you will have a docker container tagged `cloud-sync/test`.
Start the container with:

`docker run -P -d -v <swift-s3-sync checkout>:/swift-s3-sync cloud-sync/test`.
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
from an image named `cloud-sync/test`. You can override that behavior by
specifying the test container to use with the `TEST_CONTAINER` environment
variable.

The tests create and destroy the containers and buckets configured in the
`swift-s3-sync.conf` file. If you need to examine the state of a container,
consider commenting out the `tearDownClass` method to be a NOOP (TODO: add a way
to keep state).

If you would like to examine the logs from each of the services, all logs are in
/var/log (e.g. /var/log/swift-s3-sync.log).
