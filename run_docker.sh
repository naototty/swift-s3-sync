#!/bin/bash
DOCKER_ID=$(docker run --rm -P -d -v `pwd`:/swift-s3-sync swift-s3-sync)
echo ${DOCKER_ID:0:12}
docker port $DOCKER_ID

