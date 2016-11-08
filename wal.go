package merkle

import (
	"encoding/base64"
	"io"
	"strconv"
	"strings"
	"time"

	auto "github.com/tendermint/go-autofile"
	. "github.com/tendermint/go-common"
	dbm "github.com/tendermint/go-db"
	"github.com/tendermint/go-wire"
)

/*
WAL messages are always appended to an AutoFile.  The AutoFile Group manages
rolling of WAL messages.

Actions to be performed are written as WAL messages, zero or more messages per
batch. Operational messages within a batch are idempotent.

Before the Merkle tree is modified at the underlying DB layer with Save(), new
nodes and orphaned nodes are written to the WAL.  The producer of these
messages writes messages like so:

> BatchHead			height
> AddNode				key, value
> AddNode				key, value
> ...
> DelNode				key
> DelNode				key
> ...
> BatchEnd			height

Failures may happen at any time, so it is possible for some BatchHead's to be
unmatched and repeated (restarted at the same height).

> BatchHead			height
> ...
> BatchHead			height
> ...
> BatchEnd			height

The processor also writes messages as it processes batches.  These  messages
are interwoven in the same file, so might look like the following:

> BatchHead 		height
> AddNode 			key, value
> BatchStarted  height-10     <--
> AddNode   		key, value
> ...
> DelNode   		key
> BatchEnded    height-10     <--
> BatchStarted  height-9    	<--
> ...

It's possible for BatchStarted to be unmatched and repeated.
*/

//--------------------------------------------------------------------------------
// types and functions for savings consensus messages

type WALMessage interface{}

var _ = wire.RegisterInterface(
	struct{ WALMessage }{},
	wire.ConcreteType{walMsgBatchHead{}, 0x01},    // Beginning of msgs for batch
	wire.ConcreteType{walMsgBatchFoot{}, 0x02},    // End of msgs for batch
	wire.ConcreteType{walMsgAddNode{}, 0x03},      // Operation: Create a node
	wire.ConcreteType{walMsgDelNode{}, 0x04},      // Operation: Delete a node
	wire.ConcreteType{walMsgBatchStarted{}, 0x05}, // Processing of batch started
	wire.ConcreteType{walMsgBatchEnded{}, 0x06},   // Processing of batch ended
)

type walMsgBatchHead struct {
	Height int
}

type walMsgBatchFoot struct {
	Height int
}

type walMsgAddNode struct {
	Key   []byte
	Value []byte
}

type walMsgDelNode struct {
	Key []byte
}

type walMsgBatchStarted struct {
	Height int
}

type walMsgBatchEnded struct {
	Height int
}

//----------------------------------------

const (
	walMarkerBatchFoot  = "#BF:"
	walMarkerBatchEnded = "#BE:"
)

//--------------------------------------------------------------------------------

type WAL struct {
	BaseService

	group *auto.Group
	db    dbm.DB
}

func NewWAL(walDir string, db dbm.DB) (*WAL, error) {
	head, err := auto.OpenAutoFile(walDir + "/wal")
	if err != nil {
		return nil, err
	}
	group, err := auto.OpenGroup(head)
	if err != nil {
		return nil, err
	}
	wal := &WAL{
		group: group,
		db:    db,
	}
	wal.BaseService = *NewBaseService(nil, "WAL", wal)
	return wal, nil
}

func (wal *WAL) OnStart() error {
	// Run the processor
	go wal.processRoutine()
	return nil
}

func (wal *WAL) OnStop() {
	wal.BaseService.OnStop()
	wal.group.Head.Close()
	wal.group.Close()
	return
}

func (wal *WAL) write(msg WALMessage) {
	// We need to b64 encode them, newlines not allowed within message
	var msgBytes = wire.BinaryBytes(struct{ WALMessage }{msg})
	var msgBytesB64 = base64.StdEncoding.EncodeToString(msgBytes)
	err := wal.group.WriteLine(string(msgBytesB64))
	if err != nil {
		panic(Fmt("Error writing msg to WAL: %v \n\nMessage: %v", err, msg))
	}

	// Write markers for certain messages
	switch msg := msg.(type) {
	case walMsgBatchFoot:
		err := wal.group.WriteLine(walMarkerBatchFoot + Fmt("%v", msg.Height))
		if err != nil {
			panic(Fmt("Error writing BatchFoot marker to WAL: %v", err))
		}
	case walMsgBatchEnded:
		err := wal.group.WriteLine(walMarkerBatchEnded + Fmt("%v", msg.Height))
		if err != nil {
			panic(Fmt("Error writing BatchEnded marker to WAL: %v", err))
		}
	}
}

// This routine will process batches, creating new nodes
// and deleting orphaned nodes.
func (wal *WAL) processRoutine() {

	var gr *auto.GroupReader
	var height int
	var err error

	// Create a reader to stream msgs
	// Search for latest BatchEnded message, and play from there.
	match, found, err := wal.group.FindLast(walMarkerBatchEnded)
	if err != nil {
		panic(err)
	}
	if !found {
		// No BatchEnded has been found, so presumably this is a new WAL.
		gr, err = wal.group.NewReader(0)
		if err != nil {
			panic(Fmt("Error loading WAL file index 0 for processing: %v", err))
		}
	} else {
		// Search again to get the reader for matched marker.
		height, err = strconv.Atoi(match[len(walMarkerBatchEnded):])
		if err != nil {
			panic(Fmt("Error parsing BatchEnded marker: %v", err))
		}
		var found bool
		gr, found, err = wal.group.Search(walMarkerBatchEnded,
			auto.MakeSimpleSearchFunc(walMarkerBatchEnded, height),
		)
		if err != nil {
			panic(Fmt("Error searching for BatchEnded@%v: %v", height, err))
		}
		if !found {
			panic(Fmt("Expected BatchEnded@%v, but found something else", height))
		}
	}

	// Processor collects batch msgs for height before proceeding
	batchMsgs := []WALMessage{}

	// Read every line and process each block after reading it all.
	for {
		if !wal.IsRunning() {
			break
		}

		// Read a line
		line, err := gr.ReadLine()
		if err != nil {
			if err == io.EOF {
				// Nothing more to read, sleep for a bit
				time.Sleep(time.Second)
				continue
			} else {
				panic(Fmt("Error reading line from WAL: %v", err))
			}
		}

		// If line is a marker, ignore it.
		if strings.HasPrefix(line, "#") {
			continue
		}

		// Parse message
		line = strings.TrimRight(line, "\n")
		msgBytes, err := base64.StdEncoding.DecodeString(line)
		if err != nil {
			panic(Fmt("Error parsing line from WAL: %v", err))
		}
		var msg WALMessage
		err = wire.ReadBinaryBytes(msgBytes, &msg)
		if err != nil {
			panic(Fmt("Error parsing line from WAL: %v", err))
		}

		// Process message
		switch msg := msg.(type) {
		case walMsgBatchHead:
			if height == msg.Height {
				// It is possible for a batch to be opened multiple times.
				// Just reset the batch.
				batchMsgs = []WALMessage{}
			} else if height+1 != msg.Height {
				panic(Fmt("Unexpected BatchHead height %v, expected %v", msg.Height, height+1))
			} else if len(batchMsgs) != 0 {
				panic(Fmt("Variable batchMsgs has unexpected msgs"))
			}
			height = msg.Height
		case walMsgBatchFoot:
			if height != msg.Height {
				panic(Fmt("Unexpected BatchFoot height %v, expected %v", msg.Height, height))
			}
			// Process batchMsgs
			wal.processWALMessages(height, batchMsgs)
			// Clear batchMsgs
			batchMsgs = []WALMessage{}
		case walMsgAddNode, walMsgDelNode:
			batchMsgs = append(batchMsgs, msg)
		}
	}
}

func (wal *WAL) processWALMessages(height int, msgs []WALMessage) {
	// Write BatchEnded
	wal.write(walMsgBatchStarted{height})

	// Process messages
	for _, msg := range msgs {
		switch msg := msg.(type) {
		case walMsgAddNode:
			wal.db.Set(msg.Key, msg.Value)
		case walMsgDelNode:
			wal.db.Delete(msg.Key)
		}
	}

	// Write BatchEnded
	wal.write(walMsgBatchEnded{height})

	// Flush db
	// TODO: need an official API
	// TODO: verify that this does what we think it does.
	wal.db.SetSync(nil, nil)
}

//----------------------------------------

func (wal *WAL) StartHeight(height int) {
	wal.write(walMsgBatchHead{height})
}

func (wal *WAL) AddNode(key []byte, value []byte) {
	wal.write(walMsgAddNode{key, value})
}

func (wal *WAL) DelNode(key []byte) {
	wal.write(walMsgDelNode{key})
}

func (wal *WAL) EndHeightSync(height int) {
	wal.write(walMsgBatchFoot{height})
	wal.group.Head.Sync()
}
