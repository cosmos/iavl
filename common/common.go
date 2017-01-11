package common

import (
	"io"
	"os"
)

var prefix []byte = []byte{0x01} //Prefix byte for all data saved in the merkle tree, used to prevent key collision with the savekey record

//append the prefix before the key, used to prevent db collisions
func AddPrefix(key []byte) []byte {
	return append(prefix, key...)
}

func IsDirEmpty(name string) (bool, error) {
	f, err := os.Open(name)
	if err != nil {
		return true, err //folder is non-existent
	}
	defer f.Close()

	_, err = f.Readdirnames(1) // Or f.Readdir(1)
	if err == io.EOF {
		return true, nil
	}
	return false, err // Either not empty or error, suits both cases
}
