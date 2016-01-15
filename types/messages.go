package types

import "github.com/tendermint/go-wire"

const (
	RequestTypeEcho   = byte(0x01)
	RequestTypeFlush  = byte(0x02)
	ResponseTypeEcho  = byte(0x11)
	ResponseTypeFlush = byte(0x12)
)

//----------------------------------------

type RequestEcho struct {
	Message string
}

type RequestFlush struct {
}

type Request interface {
	AssertRequestType()
}

func (_ RequestEcho) AssertRequestType()  {}
func (_ RequestFlush) AssertRequestType() {}

var _ = wire.RegisterInterface(
	struct{ Request }{},
	wire.ConcreteType{RequestEcho{}, RequestTypeEcho},
	wire.ConcreteType{RequestFlush{}, RequestTypeFlush},
)

//----------------------------------------

type ResponseEcho struct {
	Message string
}

type ResponseFlush struct {
}

type Response interface {
	AssertResponseType()
}

func (_ ResponseEcho) AssertResponseType()  {}
func (_ ResponseFlush) AssertResponseType() {}

var _ = wire.RegisterInterface(
	struct{ Response }{},
	wire.ConcreteType{ResponseEcho{}, ResponseTypeEcho},
	wire.ConcreteType{ResponseFlush{}, ResponseTypeFlush},
)
