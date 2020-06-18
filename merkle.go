package iavl

import (
	"bytes"
	"encoding/hex"
	"net/url"
	"strings"

	"github.com/pkg/errors"
)

// ProofOperator is a layer for calculating intermediate Merkle roots
// when a series of Merkle trees are chained together.
// Run() takes leaf values from a tree and returns the Merkle
// root for the corresponding tree. It takes and returns a list of bytes
// to allow multiple leaves to be part of a single proof, for instance in a range proof.
// ProofOp() encodes the ProofOperator in a generic way so it can later be
// decoded with OpDecoder.
type ProofOperator interface {
	Run([][]byte) ([][]byte, error)
	GetKey() []byte
	ProofOp() ProofOp
}

// ProofOperators is a slice of ProofOperator(s).
// Each operator will be applied to the input value sequentially
// and the last Merkle root will be verified with already known data
type ProofOperators []ProofOperator

func (poz ProofOperators) VerifyValue(root []byte, keypath string, value []byte) (err error) {
	return poz.Verify(root, keypath, [][]byte{value})
}

func (poz ProofOperators) Verify(root []byte, keypath string, args [][]byte) (err error) {
	keys, err := KeyPathToKeys(keypath)
	if err != nil {
		return
	}

	for i, op := range poz {
		key := op.GetKey()
		if len(key) != 0 {
			if len(keys) == 0 {
				return errors.Errorf("key path has insufficient # of parts: expected no more keys but got %+v", string(key))
			}

			lastKey := keys[len(keys)-1]
			if !bytes.Equal(lastKey, key) {
				return errors.Errorf("key mismatch on operation #%d: expected %+v but got %+v", i, string(lastKey), string(key))
			}

			keys = keys[:len(keys)-1]
		}

		args, err = op.Run(args)
		if err != nil {
			return
		}
	}

	if !bytes.Equal(root, args[0]) {
		return errors.Errorf("calculated root hash is invalid: expected %+v but got %+v", root, args[0])
	}

	if len(keys) != 0 {
		return errors.New("keypath not consumed all")
	}

	return nil
}

// Decode a path to a list of keys. Path must begin with `/`.
// Each key must use a known encoding.
func KeyPathToKeys(path string) (keys [][]byte, err error) {
	if path == "" || path[0] != '/' {
		return nil, errors.New("key path string must start with a forward slash '/'")
	}

	parts := strings.Split(path[1:], "/")
	keys = make([][]byte, len(parts))

	for i, part := range parts {
		if strings.HasPrefix(part, "x:") {
			hexPart := part[2:]

			key, err := hex.DecodeString(hexPart)
			if err != nil {
				return nil, errors.Wrapf(err, "decoding hex-encoded part #%d: /%s", i, part)
			}

			keys[i] = key
		} else {
			key, err := url.PathUnescape(part)
			if err != nil {
				return nil, errors.Wrapf(err, "decoding url-encoded part #%d: /%s", i, part)
			}

			keys[i] = []byte(key) // TODO Test this with random bytes, I'm not sure that it works for arbitrary bytes...
		}
	}

	return keys, nil
}
