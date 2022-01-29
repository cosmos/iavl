# This docker image is designed to be used in CI for benchmarks and also by developers wanting an environment that always has the lastest rocksdb and cleveldb. 
FROM faddat/archlinux

ARG GOFLAGS=""
ENV GOFLAGS=$GOFLAGS
ENV GO111MODULE=on

RUN pacman -Syyu --noconfirm leveldb rocksdb go base-devel

COPY . .

RUN go install -tags cleveldb,rocksdb,badgerdb cmd/...
