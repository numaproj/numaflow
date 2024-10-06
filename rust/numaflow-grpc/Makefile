SHELL:=/bin/bash

.PHONY: generate
generate:
	rm -rf proto
	rm -rf src/clients/*.rs
	cp -r ../../pkg/apis/proto proto
	mv src/clients.rs /tmp/clients.rs.bak
	> src/clients.rs
	-RUST_BACKTRACE=1 cargo run
	-cargo fmt
	mv /tmp/clients.rs.bak src/clients.rs
	rm -r proto
