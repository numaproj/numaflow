SHELL:=/bin/bash

.PHONY: generate
generate:
	$(MAKE) --directory numaflow-models generate
	$(MAKE) --directory numaflow-pb generate

.PHONY: build
build:
	cargo build --release

.PHONY: test
test:
	cargo test

.PHONY: all-tests
all-tests:
	cargo test --features nats-tests,pulsar-tests,sqs-tests,kafka-tests,redis-tests --workspace --all
	cargo test --features global-state-tests test_map_stream_with_panic
	# cargo test --features global-state-tests test_batch_map_with_panic
	cargo test --features global-state-tests test_map_stream_panic -- --no-capture
	cargo test --features global-state-tests test_transform_stream_with_panic

.PHONY: clean
clean:
	cargo clean
