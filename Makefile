.PHONY: get_deps all bench test

all: test

test:
	go test `glide novendor`

bench:
	rm -rf ./test.db
	go test -bench .

get_deps:
	go get github.com/Masterminds/glide
	rm -rf ./vendor
	glide install
