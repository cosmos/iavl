package types

import "github.com/tendermint/go-wire"

const (
	RequestTypeEcho     = byte(0x01)
	RequestTypeFlush    = byte(0x02)
	RequestTypeGetValue = byte(0x03)
	RequestTypeSetValue = byte(0x04)
	RequestTypeRemValue = byte(0x05)
	RequestTypeGetHash  = byte(0x06)

	ResponseTypeException = byte(0x10)
	ResponseTypeEcho      = byte(0x11)
	ResponseTypeFlush     = byte(0x12)
	ResponseTypeGetValue  = byte(0x13)
	ResponseTypeSetValue  = byte(0x14)
	ResponseTypeRemValue  = byte(0x15)
	ResponseTypeGetHash   = byte(0x16)
)

//----------------------------------------

type RequestEcho struct {
	Message string
}

type RequestFlush struct {
}

type RequestGetValue struct {
	Key []byte // Key bytes
}

type RequestSetValue struct {
	Key   []byte // Key bytes
	Value []byte // Value bytes
}

type RequestRemValue struct {
	Key []byte // Key bytes
}

type RequestGetHash struct {
}

type Request interface {
	AssertRequestType()
}

func (_ RequestEcho) AssertRequestType()     {}
func (_ RequestFlush) AssertRequestType()    {}
func (_ RequestGetValue) AssertRequestType() {}
func (_ RequestSetValue) AssertRequestType() {}
func (_ RequestRemValue) AssertRequestType() {}
func (_ RequestGetHash) AssertRequestType()  {}

var _ = wire.RegisterInterface(
	struct{ Request }{},
	wire.ConcreteType{RequestEcho{}, RequestTypeEcho},
	wire.ConcreteType{RequestFlush{}, RequestTypeFlush},
	wire.ConcreteType{RequestGetValue{}, RequestTypeGetValue},
	wire.ConcreteType{RequestSetValue{}, RequestTypeSetValue},
	wire.ConcreteType{RequestRemValue{}, RequestTypeRemValue},
	wire.ConcreteType{RequestGetHash{}, RequestTypeGetHash},
)

//----------------------------------------

type ResponseException struct {
	Error string
}

type ResponseEcho struct {
	Message string
}

type ResponseFlush struct {
}

type ResponseGetValue struct {
	RetCode
	Value []byte // Value bytes
	Ok    bool   // True if key exists
}

type ResponseSetValue struct {
	RetCode
}

type ResponseRemValue struct {
	RetCode
	Ok bool // True if key was removed
}

type ResponseGetHash struct {
	RetCode
	Hash []byte
}

type Response interface {
	AssertResponseType()
}

func (_ ResponseException) AssertResponseType() {}
func (_ ResponseEcho) AssertResponseType()      {}
func (_ ResponseFlush) AssertResponseType()     {}
func (_ ResponseGetValue) AssertResponseType()  {}
func (_ ResponseSetValue) AssertResponseType()  {}
func (_ ResponseRemValue) AssertResponseType()  {}
func (_ ResponseGetHash) AssertResponseType()   {}

var _ = wire.RegisterInterface(
	struct{ Response }{},
	wire.ConcreteType{ResponseException{}, ResponseTypeException},
	wire.ConcreteType{ResponseEcho{}, ResponseTypeEcho},
	wire.ConcreteType{ResponseFlush{}, ResponseTypeFlush},
	wire.ConcreteType{ResponseGetValue{}, ResponseTypeGetValue},
	wire.ConcreteType{ResponseSetValue{}, ResponseTypeSetValue},
	wire.ConcreteType{ResponseRemValue{}, ResponseTypeRemValue},
	wire.ConcreteType{ResponseGetHash{}, ResponseTypeGetHash},
)
