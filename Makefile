.PHONY: build proto

build:
	go build -v ./...
test: build
	go test github.com/atomix/atomix-go-node/pkg/atomix
proto:
	docker run -it -v `pwd`:/go/src/github.com/atomix/atomix-api \
		-w /go/src/github.com/atomix/atomix-api \
		--entrypoint build/bin/compile_protos.sh \
		onosproject/protoc-go:stable
