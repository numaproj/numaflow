SHELL:=/bin/bash

clean:
	rm -rf proto

.PHONY: generate
generate: clean
	rm -rf src/clients/*.rs
	cp -r ../../pkg/apis/proto proto
	mv src/clients.rs /tmp/clients.rs.bak
	> src/clients.rs
	-./codegen.sh
	mv /tmp/clients.rs.bak src/clients.rs
	$(MAKE) clean
