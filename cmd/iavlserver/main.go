package main

import (
	"context"
	"flag"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	rt "runtime"

	"syscall"

	"github.com/gogo/gateway"
	grpc_recovery "github.com/grpc-ecosystem/go-grpc-middleware/recovery"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"github.com/pkg/errors"
	dbm "github.com/tendermint/tm-db"
	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"

	pb "github.com/cosmos/iavl/proto"
	"github.com/cosmos/iavl/server"
)

var (
	dbDataDir       = flag.String("datadir", "", "The database data directory")
	dbName          = flag.String("db-name", "", "The database name")
	dbBackend       = flag.String("db-backend", string(dbm.GoLevelDBBackend), "The database backend")
	version         = flag.Int64("version", 0, "The IAVL version to load")
	cacheSize       = flag.Int64("cache-size", 10000, "Tree cache size")
	gRPCEndpoint    = flag.String("grpc-endpoint", "localhost:8090", "The gRPC server endpoint (host:port)")
	gatewayEndpoint = flag.String("gateway-endpoint", "localhost:8091", "The gRPC-Gateway server endpoint (host:port)")
	noGateway       = flag.Bool("no-gateway", false, "Disables the gRPC-Gateway server")
	withProfiling   = flag.Bool("with-profiling", false, "Enable the pprof server")
)

var log grpclog.LoggerV2

func init() {
	log = grpclog.NewLoggerV2(os.Stdout, ioutil.Discard, ioutil.Discard)
	grpclog.SetLoggerV2(log)
}

func main() {

	rt.SetBlockProfileRate(1)

	flag.Parse()

	// start gRPC-gateway process
	go func() {
		if !(*noGateway) {
			if err := startRPCGateway(); err != nil {
				log.Fatal(err)
			}
		}
	}()

	// start gRPC (blocking) process
	listener, err := net.Listen("tcp", *gRPCEndpoint)
	if err != nil {
		log.Fatalf("failed to listen on %s: %s", *gRPCEndpoint, err)
	}

	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(grpc_recovery.UnaryServerInterceptor()),
		grpc.StreamInterceptor(grpc_recovery.StreamServerInterceptor()),
	)

	db, err := openDB()
	if err != nil {
		log.Fatalf("failed to open DB: %s", err)
	}

	svr, err := server.New(db, *cacheSize, *version)
	if err != nil {
		log.Fatalf("failed to create IAVL server: %s", err)
	}

	pb.RegisterIAVLServiceServer(grpcServer, svr)

	trapSignal(func() {
		log.Info("performing cleanup...")
		grpcServer.GracefulStop()
	})

	log.Infof("gRPC server starting on %s", *gRPCEndpoint)

	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("gRPC server terminated: %s", err)
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
		// This is necessary to get error details properly marshaled in unary requests.
		runtime.WithProtoErrorHandler(runtime.DefaultHTTPProtoErrorHandler),
	)

	dialOpts := []grpc.DialOption{grpc.WithInsecure(), grpc.WithBlock()}

	err := pb.RegisterIAVLServiceHandlerFromEndpoint(
		context.Background(), gatewayMux, *gRPCEndpoint, dialOpts,
	)
	if err != nil {
		return errors.Wrap(err, "failed to register IAVL service handler for gRPC-gateway")
	}

	r := http.NewServeMux()
	r.Handle("/", gatewayMux)

	// Register pprof handlers
	if *withProfiling {
		r.HandleFunc("/debug/pprof/", pprof.Index)
		r.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		r.HandleFunc("/debug/pprof/profile", pprof.Profile)
		r.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		r.HandleFunc("/debug/pprof/trace", pprof.Trace)
	}

	log.Infof("gRPC-gateway server starting on %s", *gatewayEndpoint)

	handlerWithPanicMW := panicRecovery(r)

	httpServer := &http.Server{
		Addr:    *gatewayEndpoint,
		Handler: handlerWithPanicMW,
	}

	if err := httpServer.ListenAndServe(); err != nil {
		return errors.Wrap(err, "gRPC-gateway server terminated")
	}

	return nil
}

func openDB() (dbm.DB, error) {
	var err error
	var db dbm.DB

	switch {
	case *dbName == "":
		return nil, errors.New("database name cannot be empty")

	case *dbBackend == "":
		return nil, errors.New("database backend cannot be empty")

	case *dbDataDir == "":
		return nil, errors.New("database datadir cannot be empty")
	}

	db, err = dbm.NewDB(*dbName, dbm.BackendType(*dbBackend), *dbDataDir)

	if err != nil {
		return nil, err
	}

	return db, err
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

// panic recovery middleware, if the handler throws a panic then this will write a 500
// response
func panicRecovery(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, rq *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				log.Error(err)

				// To avoid 'superfluous response.WriteHeader call' error
				if rw.Header().Get("Content-Type") == "" {
					rw.WriteHeader(http.StatusInternalServerError)
				}
			}
		}()

		handler.ServeHTTP(rw, rq)
	})
}
