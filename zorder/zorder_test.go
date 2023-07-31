package zorder

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"
)

func Test_getInt64Bit(t *testing.T) {
	type args struct {
		n int64
		i int
	}
	tests := []struct {
		name string
		args args
		want byte
	}{
		{"1", args{0x01, 0}, 1},
		{"2", args{0x01, 1}, 0},
		{"3", args{0x02, 0}, 0},
		{"4", args{0x02, 1}, 1},
		{"5", args{0x03, 0}, 1},
		{"6", args{0x03, 1}, 1},
		{"7", args{0x03, 2}, 0},
		{"8", args{0x03, 3}, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getInt64Bit(tt.args.n, tt.args.i); got != tt.want {
				t.Errorf("getInt64Bit() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getBytesBit(t *testing.T) {
	type args struct {
		b []byte
		i int
	}
	tests := []struct {
		name string
		args args
		want byte
	}{
		{"1", args{[]byte{0x01}, 0}, 1},
		{"2", args{[]byte{0x01}, 1}, 0},
		{"3", args{[]byte{0x02}, 0}, 0},
		{"4", args{[]byte{0x02}, 1}, 1},
		{"5", args{[]byte{0x03}, 0}, 1},
		{"6", args{[]byte{0x03}, 1}, 1},
		{"7", args{[]byte{0x03}, 2}, 0},
		{"8", args{[]byte{0x03}, 3}, 0},
		{"9", args{[]byte{0x0, 0x01}, 8}, 1},
		{"10", args{[]byte{0x0, 0x01}, 9}, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getBytesBit(tt.args.b, tt.args.i); got != tt.want {
				t.Errorf("getBytesBit() = %v, want %v", got, tt.want)
			}
		})
	}
}

func bitPrinter(bz []byte) {
	for _, b := range bz {
		for i := 0; i < 8; i++ {
			fmt.Printf("%d", (b>>i)&0x01)
		}
		fmt.Printf(" ")
	}
}

func Test_Interleave(t *testing.T) {
	type args struct {
		key     []byte
		version int64
	}
	tests := []struct {
		name string
		args args
		want []byte
	}{
		{"1", args{[]byte{0x01}, 0x01}, []byte{0x03, 0, 0, 0, 0, 0, 0, 0, 0}},
		{"1", args{[]byte{'a', 'b', 'c'}, 0x07}, []byte{0x2b, 0x14, 0x4, 0x14, 0x5, 0x14, 0x0, 0x0, 0x0, 0x0, 0x00}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var versionBz [8]byte
			binary.LittleEndian.PutUint64(versionBz[:], uint64(tt.args.version))
			got := Interleave(tt.args.key, tt.args.version)
			fmt.Print("key:     ")
			bitPrinter(tt.args.key)
			fmt.Print("\nversion: ")
			bitPrinter(versionBz[:])
			fmt.Print("\ngot:     ")
			bitPrinter(got)
			fmt.Println()
			require.Equal(t, tt.want, got)
		})
	}
}

/*
key:     10000110 01000110 11000110
version: 11100000 00000000 00000000 00000000 00000000 00000000 00000000 00000000
got:     11010100 00101000 00100000 00101000 10100000 00101000 00000000 00000000 00000000 00000000 00000000
verify:  11010100 00101000 00100000 00101000 10100000 00101000

*/

func Test_ZOrder(t *testing.T) {
	type keyVersion struct {
		key     string
		version int64
	}
	tests := []struct {
		name    string
		args    []keyVersion
		ordered []keyVersion
	}{
		{"1",
			[]keyVersion{
				{"abb", 2},
				{"abc", 2},
				{"abc", 1},
				{"abc", 3},
				{"abd", 1},
			},
			[]keyVersion{
				{"abb", 1},
				{"abc", 2},
				{"abc", 1},
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var zkeys [][]byte
			lookup := make(map[[11]byte]keyVersion)
			for _, kv := range tt.args {
				zk := Interleave([]byte(kv.key), kv.version)
				lk := [11]byte{}
				copy(lk[:], zk)
				lookup[lk] = kv
				zkeys = append(zkeys, zk)
			}

			slices.SortFunc(zkeys, func(i, j []byte) bool {
				return bytes.Compare(i, j) < 0
			})

			for _, zk := range zkeys {
				lk := [11]byte{}
				copy(lk[:], zk)
				kv := lookup[lk]
				fmt.Println(kv)
			}

			require.Equal(t, tt.ordered, zkeys)
		})
	}
}
