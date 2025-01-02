SHELL:=/bin/bash

clean:
	rm -rf proto

.PHONY: generate
generate: clean
	rm -rf src/clients/*.rs
	rm -rf src/objects/*.rs
	cp -r ../../pkg/apis/proto proto
	mkdir -p proto/google/api
	curl -Ls https://raw.githubusercontent.com/googleapis/googleapis/master/google/api/annotations.proto -o proto/google/api/annotations.proto
	curl -Ls https://raw.githubusercontent.com/googleapis/googleapis/master/google/api/http.proto -o proto/google/api/http.proto
	mv src/clients.rs /tmp/clients.rs.bak
	mv src/objects.rs /tmp/objects.rs.bak
	> src/clients.rs
	> src/objects.rs
	-./codegen.sh
	mv /tmp/clients.rs.bak src/clients.rs
	mv /tmp/objects.rs.bak src/objects.rs
	$(MAKE) clean
