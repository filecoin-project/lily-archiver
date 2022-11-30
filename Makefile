SHELL=/usr/bin/env bash

COMMIT := $(shell git rev-parse --short=8 HEAD)
DOCKER_IMAGE_NAME?=filecoin/lily-archiver
DOCKER_IMAGE_TAG?=$(COMMIT)
DOCKER_IMAGE_TAG_SUFFIX?=

.PHONY: build
build:
	go build $(GOFLAGS) -o lily-archiver -mod=readonly .

.PHONY: test
test:
	go test ./... -v

.PHONY: clean
clean:
	go clean

mainnet: build

2k: GOFLAGS+=-tags=2k
2k: build

calibnet: GOFLAGS+=-tags=calibnet
calibnet: build

nerpanet: GOFLAGS+=-tags=nerpanet
nerpanet: build

butterflynet: GOFLAGS+=-tags=butterflynet
butterflynet: build

interopnet: GOFLAGS+=-tags=interopnet
interopnet: build

.PHONY: docker
docker:
	docker build -f Dockerfile \
		--build-arg GOFLAGS=$(GOFLAGS) \
		-t $(DOCKER_IMAGE_NAME):latest$(DOCKER_IMAGE_TAG_SUFFIX) \
		-t $(DOCKER_IMAGE_NAME):$(DOCKER_IMAGE_TAG)$(DOCKER_IMAGE_TAG_SUFFIX) \
		.

.PHONY: docker-mainnet
docker-mainnet: docker

.PHONY: docker-calibnet
docker-calibnet: GOFLAGS+=-tags=calibnet
docker-calibnet: DOCKER_IMAGE_TAG_SUFFIX:=-calibnet
docker-calibnet: docker

.PHONY: docker-2k
docker-2k: GOFLAGS+=-tags=2k
docker-2k: DOCKER_IMAGE_TAG_SUFFIX:=-2k
docker-2k: docker

.PHONY: docker-nerpanet
docker-nerpanet: GOFLAGS+=-tags=nerpanet
docker-nerpanet: DOCKER_IMAGE_TAG_SUFFIX:=-nerpanet
docker-nerpanet: docker

.PHONY: docker-butterflynet
docker-butterflynet: GOFLAGS+=-tags=butterflynet
docker-butterflynet: DOCKER_IMAGE_TAG_SUFFIX:=-butterflynet
docker-butterflynet: docker

.PHONY: docker-interopnet
docker-interopnet: GOFLAGS+=-tags=interopnet
docker-interopnet: DOCKER_IMAGE_TAG_SUFFIX:=-interopnet
docker-interopnet: docker


