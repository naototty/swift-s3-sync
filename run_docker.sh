#!/bin/bash
DOCKER_ID=$(docker run --rm -P -d -v `pwd`:/swift-s3-sync swift-s3-sync)
echo $DOCKER_ID
docker port $DOCKER_ID

