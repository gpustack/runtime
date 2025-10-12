.SILENT:
.DEFAULT_GOAL := ci
.PHONY: docs

SHELL := /bin/bash

SRCDIR := $(patsubst %/,%,$(dir $(abspath $(lastword $(MAKEFILE_LIST)))))

GIT_COMMIT ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
prepare:
	echo "git_commit = \"$(GIT_COMMIT)\"" > $(SRCDIR)/gpustack_runtime/_version_appendix.py

deps: prepare
	@echo "+++ $@ +++"
	uv sync --all-packages
	uv lock
	uv tree
	@echo "--- $@ ---"

INSTALL_HOOKS ?= true
install: deps
	@echo "+++ $@ +++"
	if [[ "$(INSTALL_HOOKS)" == "true" ]]; then \
		uv run pre-commit install --hook-type pre-commit --hook-type commit-msg --hook-type pre-push; \
	else \
		echo "Skipping pre-commit hook installation."; \
	fi
	@echo "--- $@ ---"

clean:
	@echo "+++ $@ +++"
	uv run pyclean -v $(SRCDIR)
	rm -rf dist
	@echo "--- $@ ---"

LINT_DIRTY ?= false
lint:
	@echo "+++ $@ +++"
	uv run pre-commit run --all-files --show-diff-on-failure
	if [[ "$(LINT_DIRTY)" == "true" ]]; then \
		if [[ -n $$(git status --porcelain) ]]; then \
			echo "Code tree is dirty."; \
			git diff --exit-code; \
		fi; \
	fi
	@echo "--- $@ ---"

test:
	@echo "+++ $@ +++"
	uv run pytest
	@echo "--- $@ ---"

build: prepare
	@echo "+++ $@ +++"
	rm -rf dist
	uv build
	@echo "--- $@ ---"

docs:
	@echo "+++ $@ +++"
	rm -rf site
	uv run mkdocs build
	@echo "--- $@ ---"

docs-online: docs
	@echo "+++ $@ +++"
	uv run mkdocs serve -o -w $(SRCDIR)/gpustack_runtime
	@echo "--- $@ ---"

PACKAGE_NAMESPACE ?= gpustack
PACKAGE_REPOSITORY ?= runtime
PACKAGE_OS ?= $(shell uname -s | tr '[:upper:]' '[:lower:]')
PACKAGE_ARCH ?= $(shell uname -m | sed 's/aarch64/arm64/' | sed 's/x86_64/amd64/')
PACKAGE_TAG ?= main
package:
	@echo "+++ $@ +++"
	if [[ -z $$(command -v docker) ]]; then \
		echo "[FATAL] Docker is not installed. Please install Docker to use this target."; \
		exit 1; \
	fi
	if [[ -z $$(docker buildx inspect --builder "gpustack" 2>/dev/null) ]]; then \
    	echo "[INFO] Creating new buildx builder 'gpustack'"; \
	    docker run --rm --privileged tonistiigi/binfmt:qemu-v9.2.2-52 --uninstall qemu-*; \
	    docker run --rm --privileged tonistiigi/binfmt:qemu-v9.2.2 --install all; \
	    docker buildx create \
	    	--name "gpustack" \
	    	--driver "docker-container" \
	    	--buildkitd-flags "--allow-insecure-entitlement security.insecure --allow-insecure-entitlement network.host" \
	    	--driver-opt "network=host,default-load=true,env.BUILDKIT_STEP_LOG_MAX_SIZE=-1,env.BUILDKIT_STEP_LOG_MAX_SPEED=-1" \
	    	--bootstrap; \
	fi
	TAG=$(PACKAGE_NAMESPACE)/$(PACKAGE_REPOSITORY):$(PACKAGE_TAG); \
	echo "[INFO] Building '$${TAG}' platform '$(PACKAGE_OS)/$(PACKAGE_ARCH)'"; \
	docker buildx build \
		--pull \
		--allow network.host \
		--allow security.insecure \
		--builder "gpustack" \
		--platform "$(PACKAGE_OS)/$(PACKAGE_ARCH)" \
		--tag "$${TAG}" \
		--file "$(SRCDIR)/pack/Dockerfile" \
		--progress plain \
		$(SRCDIR)
	@echo "--- $@ ---"

ci: deps install lint test clean build
