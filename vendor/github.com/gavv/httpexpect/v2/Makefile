all: tidy gen build lint test spell

tidy:
	go mod tidy -v 
	cd _examples && go get -v -u github.com/gavv/httpexpect/v2
	cd _examples && go mod tidy -v -compat=1.17

gen:
	go generate ./...

fmt:
	gofmt -s -w . ./e2e ./_examples
ifneq (,$(findstring GNU,$(shell sed --version)))
	sed -r -e ':loop' -e 's,^(//\t+)    ,\1\t,g' -e 't loop' -i *.go e2e/*.go _examples/*.go
endif

build:
	go build ./...
	cd _examples && go build

lint:
	golangci-lint run ./...
	cd _examples && golangci-lint run .

test:
ifneq ($(shell which gotest),)
	gotest ./...
	cd _examples && gotest
else
	go test ./...
	cd _examples && go test
endif

short:
ifneq ($(shell which gotest),)
	gotest -short ./...
else
	go test -short ./...
endif

spell:
ifneq ($(shell which mdspell),)
	mdspell -a README.md
	sort .spelling -o .spelling
endif

toc:
	markdown-toc --maxdepth 3 -i HACKING.md
