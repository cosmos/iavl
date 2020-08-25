FROM golang:1.14 as build

WORKDIR /iavl

ARG GOFLAGS=""
ENV GOFLAGS=$GOFLAGS
ENV GO111MODULE=on

# Download dependencies first - this should be cacheable.
COPY go.mod go.sum ./
RUN go mod download

# Now add the local iavl repo, which typically isn't cacheable.
COPY . .

# Build the server.
RUN go get ./cmd/iavlserver

# Make a minimal image.
FROM gcr.io/distroless/base

COPY --from=build /go/bin/iavlserver /

EXPOSE 8090 8091
ENTRYPOINT ["/iavlserver"]
CMD ["-db-name", "iavl", "-datadir", "."]
