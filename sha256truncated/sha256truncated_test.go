package sha256truncated

import (
	"crypto/sha256"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSum(t *testing.T) {
	// very rudimentary testing here showing the relating between this hasher and sha256;
	// assuming sha256 is already well tested
	s256 := sha256.New()
	s256Trunc := New()

	testStr1 := []byte("show the relation between ...")
	testStr2 := []byte("... sha256 and sha256truncated")

	s256.Write(testStr1)
	s256Trunc.Write(testStr1)

	s256.Write(testStr2)
	s256Trunc.Write(testStr2)

	require.Equal(t, s256Trunc.Sum(nil), s256.Sum(nil)[:20])
}

func TestSize(t *testing.T) {
	got := New().Size()
	want := 20
	require.Equal(t, got, want)
}
