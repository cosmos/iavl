package keyformat

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func FuzzKeyFormatKeyBytes(f *testing.F) {
	if testing.Short() {
		f.Skip("in -short mode")
	}

	// 1. Create some seeds.
	seeds := []*KeyFormat{
		NewKeyFormat('a', 1, 2, 3, 5, 6, 0),
		NewKeyFormat('b', 1, 2, 3, 5, 6, 1),
		NewKeyFormat('b', 9, 21),
		NewKeyFormat(byte('e'), 8, 8, 8),
		NewKeyFormat(byte('e'), 8, 0),
	}

	type envelope struct {
		KF *KeyFormat `json:"kf"`
		BS [][]byte   `json:"bs"`
	}

	for _, kf := range seeds {
		bsL := [][][]byte{
			[][]byte{[]byte(""), []byte("abcdefgh")},
			[][]byte{{1, 2, 3, 4, 5, 6, 7, 8}, {1, 2, 3, 4, 5, 6, 7, 8, 9}},
			[][]byte{{1, 2, 3, 4, 5, 6, 7, 8}, []byte("hellohello")},
			[][]byte{{1, 2, 3, 4, 5, 6, 7, 8}, {1, 2, 3, 4, 5, 6, 7, 8}, {1, 1, 2, 2, 3, 3}},
		}
		for _, bs := range bsL {
			blob, err := json.Marshal(&envelope{KF: kf, BS: bs})
			if err != nil {
				f.Fatal(err)
			}
			f.Add(blob)
		}
	}

	// 2. Fuzz it now.
	f.Fuzz(func(t *testing.T, inputJSON []byte) {
		defer func() {
			if r := recover(); r != nil {
				str := fmt.Sprintf("%s", r)
				if !strings.Contains(str, "provided to KeyFormat.KeyBytes() is longer than the") {
					// Re-raise the panic as it panicked in an unexpected place.
					panic(r)
				}
			}
		}()

		env := new(envelope)
		if err := json.Unmarshal(inputJSON, env); err != nil {
			return
		}

		kf := env.KF
		if kf == nil {
			return
		}
		_ = kf.KeyBytes(env.BS...)
	})
}
