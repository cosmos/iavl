package server

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strings"

	. "github.com/tendermint/go-common"
	"github.com/tendermint/go-wire"
	"github.com/tendermint/tmsp/types"
)

func StartListener(protoAddr string) (net.Listener, error) {
	parts := strings.SplitN(protoAddr, "://", 2)
	proto, addr := parts[0], parts[1]
	ln, err := net.Listen(proto, addr)
	if err != nil {
		return nil, err
	}

	// A goroutine to accept a connection.
	go func() {

		for {

			// Accept a connection
			fmt.Println("Waiting for new connection...")
			conn, err := ln.Accept()
			if err != nil {
				Exit("Failed to accept connection")
			} else {
				fmt.Println("Accepted a new connection")
			}

			closeConn := make(chan error, 2)             // Push to signal connection closed
			responses := make(chan types.Response, 1000) // A channel to buffer responses

			// Read requests from conn and deal with them
			go handleRequests(closeConn, conn, responses)
			// Pull responses from 'responses' and write them to conn.
			go handleResponses(closeConn, responses, conn)

			go func() {
				// Wait until signal to close connection
				errClose := <-closeConn
				if errClose != nil {
					fmt.Printf("Connection error: %v\n", errClose)
				} else {
					fmt.Println("Connection was closed.")
				}

				// Close the connection
				err := conn.Close()
				if err != nil {
					fmt.Printf("Error in closing connection: %v\n", err)
				}

				// <-semaphore
			}()
		}

	}()

	return ln, nil
}

// Read requests from conn and deal with them
func handleRequests(closeConn chan error, conn net.Conn, responses chan<- types.Response) {
	for {
		var n int
		var err error
		var req types.Request
		wire.ReadBinaryPtrLengthPrefixed(&req, conn, 0, &n, &err)
		if err != nil {
			if err == io.EOF {
				closeConn <- fmt.Errorf("Connection closed by client")
			} else {
				closeConn <- fmt.Errorf("Error in handleRequests: %v", err.Error())
			}
			return
		}

		// Request
		switch req := req.(type) {
		case types.RequestEcho:
			responses <- types.ResponseEcho{req.Message}
		case types.RequestFlush:
			responses <- types.ResponseFlush{}
		default:
			Exit("Unknown request type")
		}
	}
}

// Pull responses from 'responses' and write them to conn.
func handleResponses(closeConn chan error, responses <-chan types.Response, conn net.Conn) {
	var bufWriter = bufio.NewWriter(conn)
	for {
		var res = <-responses
		var n int
		var err error
		wire.WriteBinaryLengthPrefixed(struct{ types.Response }{res}, bufWriter, &n, &err)
		if err != nil {
			closeConn <- fmt.Errorf("Error in handleResponses: %v", err.Error())
			return
		}
		if _, ok := res.(types.ResponseFlush); ok {
			err = bufWriter.Flush()
			if err != nil {
				closeConn <- fmt.Errorf("Error in handleResponses: %v", err.Error())
				return
			}
		}
	}
}
