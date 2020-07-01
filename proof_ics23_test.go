package iavl

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"testing"

	ics23 "github.com/confio/ics23/go"
	"github.com/stretchr/testify/require"

	db "github.com/tendermint/tm-db"
)

func TestConvertExistence(t *testing.T) {
	proof, err := GenerateResult(200, Middle)
	require.NoError(t, err)

	converted, err := convertExistenceProof(proof.Proof, proof.Key, proof.Value)
	require.NoError(t, err)

	calc, err := converted.Calculate()
	require.NoError(t, err)

	require.Equal(t, []byte(calc), proof.RootHash, "Calculated: %X\nExpected:   %X", calc, proof.RootHash)
}

func TestCreateMembership(t *testing.T) {
	cases := map[string]struct {
		size int
		loc  Where
	}{
		"small left":   {size: 100, loc: Left},
		"small middle": {size: 100, loc: Middle},
		"small right":  {size: 100, loc: Right},
		"big left":     {size: 5431, loc: Left},
		"big middle":   {size: 5431, loc: Middle},
		"big right":    {size: 5431, loc: Right},
	}

	for name, tc := range cases {
		tc := tc
		t.Run(name, func(t *testing.T) {
			tree, allkeys, err := BuildTree(tc.size)
			require.NoError(t, err, "Creating tree: %+v", err)

			key := GetKey(allkeys, tc.loc)
			_, val := tree.Get(key)
			proof, err := CreateMembershipProof(tree, key)
			require.NoError(t, err, "Creating Proof: %+v", err)

			root := tree.Hash()
			valid := ics23.VerifyMembership(ics23.IavlSpec, root, proof, key, val)
			if !valid {
				require.NoError(t, err, "Membership Proof Invalid")
			}
		})
	}
}

func TestCreateNonMembership(t *testing.T) {
	cases := map[string]struct {
		size int
		loc  Where
	}{
		"small left":   {size: 100, loc: Left},
		"small middle": {size: 100, loc: Middle},
		"small right":  {size: 100, loc: Right},
		"big left":     {size: 5431, loc: Left},
		"big middle":   {size: 5431, loc: Middle},
		"big right":    {size: 5431, loc: Right},
	}

	for name, tc := range cases {
		tc := tc
		t.Run(name, func(t *testing.T) {
			tree, allkeys, err := BuildTree(tc.size)
			require.NoError(t, err, "Creating tree: %+v", err)

			key := GetNonKey(allkeys, tc.loc)

			proof, err := CreateNonMembershipProof(tree, key)
			require.NoError(t, err, "Creating Proof: %+v", err)

			root := tree.Hash()
			valid := ics23.VerifyNonMembership(ics23.IavlSpec, root, proof, key)
			if !valid {
				require.NoError(t, err, "Non Membership Proof Invalid")
			}
		})
	}
}

// Test Helpers

// Result is the result of one match
type Result struct {
	Key      []byte
	Value    []byte
	Proof    *RangeProof
	RootHash []byte
}

// GenerateResult makes a tree of size and returns a range proof for one random element
//
// returns a range proof and the root hash of the tree
func GenerateResult(size int, loc Where) (*Result, error) {
	tree, allkeys, err := BuildTree(size)
	if err != nil {
		return nil, err
	}
	key := GetKey(allkeys, loc)

	value, proof, err := tree.GetWithProof(key)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, fmt.Errorf("tree.GetWithProof returned nil value")
	}
	if len(proof.Leaves) != 1 {
		return nil, fmt.Errorf("tree.GetWithProof returned %d leaves", len(proof.Leaves))
	}
	root := tree.Hash()

	res := &Result{
		Key:      key,
		Value:    value,
		Proof:    proof,
		RootHash: root,
	}
	return res, nil
}

// Where selects a location for a key - Left, Right, or Middle
type Where int

const (
	Left Where = iota
	Right
	Middle
)

// GetKey this returns a key, on Left/Right/Middle
func GetKey(allkeys [][]byte, loc Where) []byte {
	if loc == Left {
		return allkeys[0]
	}
	if loc == Right {
		return allkeys[len(allkeys)-1]
	}
	// select a random index between 1 and allkeys-2
	// nolint:gosec
	idx := rand.Int()%(len(allkeys)-2) + 1
	return allkeys[idx]
}

// GetNonKey returns a missing key - Left of all, Right of all, or in the Middle
func GetNonKey(allkeys [][]byte, loc Where) []byte {
	if loc == Left {
		return []byte{0, 0, 0, 1}
	}
	if loc == Right {
		return []byte{0xff, 0xff, 0xff, 0xff}
	}
	// otherwise, next to an existing key (copy before mod)
	key := append([]byte{}, GetKey(allkeys, loc)...)
	key[len(key)-2] = 255
	key[len(key)-1] = 255
	return key
}

// BuildTree creates random key/values and stores in tree
// returns a list of all keys in sorted order
func BuildTree(size int) (itree *ImmutableTree, keys [][]byte, err error) {
	tree, _ := NewMutableTree(db.NewMemDB(), 0)

	// insert lots of info and store the bytes
	keys = make([][]byte, size)
	for i := 0; i < size; i++ {
		// create random 4 byte key
		// nolint:gosec
		key := []byte{byte(rand.Uint64()), byte(rand.Uint64()), byte(rand.Uint64()), byte(rand.Uint64())}
		value := "value_for_key:" + string(key)
		tree.Set(key, []byte(value))
		keys[i] = key
	}
	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(keys[i], keys[j]) < 0
	})

	return tree.ImmutableTree, keys, nil
}
