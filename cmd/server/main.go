package main

import (
	"context"
	"flag"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime/pprof"
	"syscall"

	"github.com/gogo/gateway"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"

	pb "github.com/tendermint/iavl/proto"
	"github.com/tendermint/iavl/server"
)

var (
	gRPCEndpoint    = flag.String("grpc-endpoint", "localhost:8090", "The gRPC server endpoint (host:port)")
	gatewayEndpoint = flag.String("gateway-endpoint", "localhost:8091", "The gRPC-Gateway server endpoint (host:port)")
	cpuProfile      = flag.String("cpuprofile", "", "If set, write CPU profile to this file")
	memProfile      = flag.String("memprofile", "", "If set, write memory profile to this file")
)

var log grpclog.LoggerV2

func init() {
	log = grpclog.NewLoggerV2(os.Stdout, ioutil.Discard, ioutil.Discard)
	grpclog.SetLoggerV2(log)
}

func main() {
	flag.Parse()

	// enable CPU profile if requested
	if *cpuProfile != "" {
		f := mustCreateFile(*cpuProfile)
		_ = pprof.StartCPUProfile(f)

		defer pprof.StopCPUProfile()
	}

	// start gRPC-gateway process
	go func() {
		if err := startRPCGateway(); err != nil {
			log.Error(err)
		}
	}()

	// start gRPC (blocking) process
	listener, err := net.Listen("tcp", *gRPCEndpoint)
	if err != nil {
		log.Errorf("failed to listen on %s: %s", *gRPCEndpoint, err)
	}

	var svrOpts []grpc.ServerOption
	grpcServer := grpc.NewServer(svrOpts...)

	pb.RegisterIAVLServer(grpcServer, server.New())

	trapSignal(func() {
		grpcServer.GracefulStop()
	})

	log.Infof("gRPC server starting on %s", *gRPCEndpoint)

	if err := grpcServer.Serve(listener); err != nil {
		log.Errorf("gRPC server terminated: %s", err)
	}

	// write memory profile if requested
	if *memProfile != "" {
		f := mustCreateFile(*memProfile)
		_ = pprof.WriteHeapProfile(f)
	}
}

// startRPCGateway starts the gRPC-gateway server. It returns an error if the
// server fails to start. The server acts as an HTTP JSON proxy to the gRPC
// server.
func startRPCGateway() error {
	jsonPb := &gateway.JSONPb{
		EmitDefaults: true,
		Indent:       "  ",
		OrigName:     true,
	}
	gatewayMux := runtime.NewServeMux(
		runtime.WithMarshalerOption(runtime.MIMEWildcard, jsonPb),
		// This is necessary to get error details properly marshalled in unary requests.
		runtime.WithProtoErrorHandler(runtime.DefaultHTTPProtoErrorHandler),
	)

	dialOpts := []grpc.DialOption{grpc.WithInsecure(), grpc.WithBlock()}

	err := pb.RegisterIAVLHandlerFromEndpoint(
		context.Background(), gatewayMux, *gRPCEndpoint, dialOpts,
	)
	if err != nil {
		return errors.Wrap(err, "failed to register IAVL service handler for gRPC-gateway")
	}

	http.Handle("/", gatewayMux)
	log.Infof("gRPC-gateway server starting on %s", *gatewayEndpoint)

	if err := http.ListenAndServe(*gatewayEndpoint, nil); err != nil {
		return errors.Wrap(err, "gRPC-gateway server terminated")
	}

	return nil
}

// trapSignal will listen for any OS signal and invokes a callback function to
// perform any necessary cleanup.
func trapSignal(cb func()) {
	var sigCh = make(chan os.Signal)

	signal.Notify(sigCh, syscall.SIGTERM)
	signal.Notify(sigCh, syscall.SIGINT)

	go func() {
		sig := <-sigCh
		log.Infof("caught signal %s; shutting down...", sig)
		cb()
	}()
}

func mustCreateFile(fileName string) *os.File {
	f, err := os.Create(fileName)
	if err != nil {
		log.Fatal(err)
	}

	return f
}
