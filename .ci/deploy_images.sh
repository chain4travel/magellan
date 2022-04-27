#!/bin/bash -e

// Copyright (C) 2022, Chain4Travel AG. All rights reserved.
//
// This file is a derived work, based on ava-labs code whose
// original notices appear below.
//
// It is distributed under the same license conditions as the
// original code from which it is derived.
//
// Much love to the original authors for their work.
// **********************************************************
# (c) 2020, Ava Labs, Inc. All rights reserved.
# See the file LICENSE for licensing terms.

# Build the image with a canonical commit-based name
IMAGE_NAME="$DOCKER_REGISTRY/magellan:dev-$(git rev-parse --short HEAD)"
DOCKER_IMAGE_NAME="$IMAGE_NAME" make image

# Login to docker registry
echo "$DOCKER_PASSWORD" | docker login --username $DOCKER_USER --password-stdin

# Create and publish all the appropriate tags
# Every commit gets a commit-based tag. Git tags, master branch, and staging
# branch all get special tags
tag_and_push () {
  docker tag $IMAGE_NAME "$DOCKER_REGISTRY/magellan:$1"
  docker push "$DOCKER_REGISTRY/magellan:$1"
}

docker push "$IMAGE_NAME"
if [ "$TRAVIS_TAG" != "" ] ; then tag_and_push $TRAVIS_TAG; fi
if [ "$TRAVIS_BRANCH" == "master" ] ; then tag_and_push "latest"; fi
if [ "$TRAVIS_BRANCH" == "staging" ] ; then tag_and_push "staging"; fi

