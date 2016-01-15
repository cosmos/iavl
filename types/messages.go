package types

import "github.com/tendermint/go-wire"

const (
	requestTypeEcho   = byte(0x01)
	requestTypeFlush  = byte(0x02)
	responseTypeEcho  = byte(0x11)
	responseTypeFlush = byte(0x12)
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
	wire.ConcreteType{RequestEcho{}, requestTypeEcho},
	wire.ConcreteType{RequestFlush{}, requestTypeFlush},
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
	wire.ConcreteType{ResponseEcho{}, responseTypeEcho},
	wire.ConcreteType{ResponseFlush{}, responseTypeFlush},
)
