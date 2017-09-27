GOTOOLS := github.com/mitchellh/gox \
           github.com/Masterminds/glide \
	   github.com/alecthomas/gometalinter

PDFFLAGS := -pdf --nodefraction=0.1

all: get_vendor_deps test

test:
	go test -v --race `glide novendor`

tools:
	go get -u -v $(GOTOOLS)

get_vendor_deps:
	go get github.com/Masterminds/glide
	glide install

# bench is the basic tests that shouldn't crash an aws instance
bench:
	cd benchmarks && \
		go test -bench=RandomBytes . && \
		go test -bench=Small . && \
		go test -bench=Medium . && \
		go test -bench=BenchmarkMemKeySizes .

# fullbench is extra tests needing lots of memory and to run locally
fullbench:
	cd benchmarks && \
		go test -bench=RandomBytes . && \
		go test -bench=Small . && \
		go test -bench=Medium . && \
		go test -timeout=30m -bench=Large . && \
		go test -bench=Mem . && \
		go test -timeout=60m -bench=LevelDB .


# note that this just profiles the in-memory version, not persistence
profile:
	cd benchmarks && \
		go test -bench=Mem -cpuprofile=cpu.out -memprofile=mem.out . && \
		go tool pprof ${PDFFLAGS} benchmarks.test cpu.out > cpu.pdf && \
		go tool pprof --alloc_space ${PDFFLAGS} benchmarks.test mem.out > mem_space.pdf && \
		go tool pprof --alloc_objects ${PDFFLAGS} benchmarks.test mem.out > mem_obj.pdf

explorecpu:
	cd benchmarks && \
		go tool pprof benchmarks.test cpu.out

exploremem:
	cd benchmarks && \
		go tool pprof --alloc_objects benchmarks.test mem.out

delve:
	dlv test ./benchmarks -- -test.bench=.

metalinter: tools
	@gometalinter --install
	gometalinter --vendor --deadline=600s --enable-all --disable=lll ./...

metalinter_test: tools
	@gometalinter --install
	gometalinter --vendor --deadline=600s --disable-all  \
		--enable=deadcode \
		--enable=errcheck \
		--enable=gas \
		--enable=goconst \
		--enable=goimports \
		--enable=gosimple \
		--enable=ineffassign \
		--enable=misspell \
		--enable=staticcheck \
		--enable=safesql \
		--enable=unconvert \
		--enable=unused \
		--enable=vetshadow \
		./...

		#--enable=aligncheck \
		#--enable=dupl \
		#--enable=gocyclo \
		#--enable=golint \ <== comments on anything exported
		#--enable=gotype \
		#--enable=interfacer \
		#--enable=megacheck \
		#--enable=structcheck \
		#--enable=unparam \
		#--enable=varcheck \
		#--enable=vet \

.PHONY: all test tools
