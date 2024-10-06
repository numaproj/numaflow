SHELL:=/bin/bash

.PHONY: codegen
codegen:
	$(MAKE) --directory numaflow-models generate
	$(MAKE) --directory numaflow-grpc generate

.PHONY: build
build:
	cargo build --release

.PHONY: test
test:
	cargo test

.PHONY: all-tests
all-tests:
	cargo test --all-features

.PHONY: clean
clean:
	cargo clean
