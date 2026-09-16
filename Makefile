# Makefile for mimersql Docker images
#
# --- Building your own image -----------------------------------------
# Anyone can build their own image with:
#
#   make build VERSION=11.0 TAG=v11.0.9g
#   docker run --rm -it mimersql/mimersql_v11.0:v11.0.9g
#
# This only builds and loads the image into your local Docker — it does
# not publish or push anything anywhere. (There's also a `push` target
# below, used for official Mimer SQL releases; running it yourself will just
# get you a permission error from Docker Hub, since publishing to the
# `mimersql` org needs Mimer Information Technology's own credentials —
# this Makefile itself doesn't grant or restrict that.)
# ------------------------------------------------------------------------
#
# More examples:
#   make push VERSION=11.1 TAG=v11.1.0
#   make push VERSION=11.1 TAG=v11.1.0 REST=1     # same, but the REST-controller variant
#   make build-multiarch VERSION=11.0 TAG=v11.0.9g PUSH=1
#
# VERSION selects which Docker Hub repo and build-args are used.
# TAG is the specific version tag (e.g. v11.0.9g). If it ends in a single
# letter (a per-build/arch suffix, e.g. the "g" in v11.0.9g), a second tag
# with that letter stripped (e.g. v11.0.9) is also created — this matches
# how we've always released, e.g. v11.0.9g + v11.0.9 + latest.
#
# `build` and `build-multiarch` create the exact same tags — `build` just
# builds for your own platform only and loads it into your local Docker so
# you can run it, while `build-multiarch` builds for amd64+arm64 and can't
# be run locally (only `--push`ed, or left in the build cache to check it
# compiles). Anyone can use `build` to build their own image; only
# Mimer Information Technology can actually publish,
# via `build-multiarch`/`push` (that needs write access to the `mimersql`
# Docker Hub org — everyone else just gets a permission error from Docker Hub,
# regardless of this Makefile).
#
# REST=1 builds/tags the REST-controller variant: every tag gets a
# "-rest" suffix (v11.0.9g-rest, v11.0.9-rest, latest-rest). This does NOT
# switch git branches for you — the REST controller lives on the "rest"
# branch, so check that out yourself first:
#   git checkout rest
#   make push VERSION=11.0 TAG=v11.0.9g REST=1
#   git checkout main
#   make push VERSION=11.0 TAG=v11.0.9g

DOCKERHUB_NS := mimersql

# --- Known versions ------------------------------------------------------
# Mimer SQL is installed as a single .deb package. VERSION decides which
# Docker Hub repo is built against and is passed on as --build-arg
# MIMER_VERSION, which in turn selects which .deb file the Dockerfile
# fetches. Add a new version by adding it to the list below.
#
# NOTE: the superuser name (MIMER_SUPERUSER) is NOT a build-time thing.
# From 11.1 it's chosen by whoever runs the image, with "-e
# MIMER_SUPERUSER=<name>" at "docker run" (see README.md), and handled by
# start.sh. It should therefore never be passed as a --build-arg.

KNOWN_VERSIONS := 11.0 11.1

# ---------------------------------------------------------------------------

REPO := $(DOCKERHUB_NS)/mimersql_v$(VERSION)

BUILD_ARGS := \
	--build-arg MIMER_VERSION=$(VERSION)

# REST=1 (or any non-empty value) tags everything with a "-rest" suffix.
REST_SUFFIX := $(if $(REST),-rest,)

# Strip a single trailing letter off TAG, e.g. v11.0.9g -> v11.0.9. Left
# unchanged if TAG doesn't end in a letter (e.g. v11.1.0).
SHORT_TAG := $(shell echo "$(TAG)" | sed -E 's/[A-Za-z]$$//')

# The tags every build produces: TAG, the letter-stripped short tag (only
# if it's actually different), and :latest — all with the -rest suffix
# applied when REST=1. Shared between `build` and `build-multiarch` so
# they can never drift apart.
TAG_ARGS := \
	-t $(REPO):$(TAG)$(REST_SUFFIX) \
	$(if $(filter $(TAG),$(SHORT_TAG)),,-t $(REPO):$(SHORT_TAG)$(REST_SUFFIX)) \
	-t $(REPO):latest$(REST_SUFFIX)
TAG_SUMMARY := $(REPO):$(TAG)$(REST_SUFFIX)$(if $(filter $(TAG),$(SHORT_TAG)),, and $(REPO):$(SHORT_TAG)$(REST_SUFFIX)) and $(REPO):latest$(REST_SUFFIX)

# The branch we'd expect to be on for the requested variant, just to catch
# the easy mistake of building/pushing the wrong variant. Warning only.
EXPECTED_BRANCH := $(if $(REST),rest,main)
CURRENT_BRANCH  := $(shell git rev-parse --abbrev-ref HEAD 2>/dev/null)

.PHONY: help build build-multiarch push check-branch check-version

help:
	@echo "Targets:"
	@echo "  make build          VERSION=11.1 TAG=v11.1.0 [REST=1]         - build for your own platform, --load'ed so you can run it"
	@echo "  make build-multiarch VERSION=11.1 TAG=v11.1.0 [PUSH=1] [REST=1] - multi-arch build, pushed if PUSH=1 is set"
	@echo "  make push           VERSION=11.1 TAG=v11.1.0 [REST=1]          - alias for build-multiarch PUSH=1"
	@echo ""
	@echo "'build' and 'build-multiarch' create the exact same tags. Anyone can"
	@echo "run 'build' to get a runnable image; only Mimer Information Technology can publish with"
	@echo "'build-multiarch'/'push' (requires Docker Hub credentials for the"
	@echo "mimersql org)."
	@echo ""
	@echo "REST=1 tags the REST-controller variant instead (adds a -rest suffix"
	@echo "to every tag). Check out the 'rest' branch yourself first."
	@echo ""
	@echo "Known versions: $(KNOWN_VERSIONS)"

check-branch:
	@if [ -n "$(CURRENT_BRANCH)" ] && [ "$(CURRENT_BRANCH)" != "$(EXPECTED_BRANCH)" ]; then \
		echo "WARNING: REST=$(REST) expects branch '$(EXPECTED_BRANCH)', but you're on '$(CURRENT_BRANCH)'."; \
	fi

check-version:
ifndef VERSION
	$(error VERSION is required, e.g. "make build VERSION=11.1")
else
ifeq ($(filter $(VERSION),$(KNOWN_VERSIONS)),)
	$(error Unknown VERSION "$(VERSION)" — add it to KNOWN_VERSIONS in the Makefile)
endif
endif

## For anyone: build your own image for your own platform only,
## --load'ed into your local Docker so you can run it. Tagged exactly
## like build-multiarch/push would tag it. This never publishes/pushes
## anything anywhere — see build-multiarch/push below for that.
build: check-version check-branch
ifndef TAG
	$(error TAG is required for build, e.g. TAG=v11.1.0)
endif
	docker buildx build --platform linux/amd64 \
		$(BUILD_ARGS) \
		$(TAG_ARGS) \
		--load .
	@echo ""
	@echo "Built: $(TAG_SUMMARY)"
	@echo "Run it with e.g.: docker run --rm -it $(REPO):$(TAG)$(REST_SUFFIX)"

## Multi-arch build. Set PUSH=1 to push, otherwise it's only built (without
## --load, i.e. it just stays in the build cache — use 'make build' for a
## locally runnable image instead). Tagged exactly like build.
build-multiarch: check-version check-branch
ifndef TAG
	$(error TAG is required for build-multiarch/push, e.g. TAG=v11.1.0)
endif
	docker buildx build --platform linux/amd64,linux/arm64 \
		$(BUILD_ARGS) \
		$(TAG_ARGS) \
		$(if $(PUSH),--push,) \
		.
	@echo ""
	@echo "Built: $(TAG_SUMMARY)"
	@if [ -z "$(PUSH)" ]; then \
		echo "NOTE: not pushed (multi-arch without --push/--load only stays in the build cache)."; \
		echo "Run with PUSH=1 to push, or use 'make build' for a locally runnable image."; \
	fi

## Convenience alias
push: check-version
	$(MAKE) build-multiarch PUSH=1 VERSION=$(VERSION) TAG=$(TAG) REST=$(REST)
