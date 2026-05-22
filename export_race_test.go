package iavl

import (
	"encoding/binary"
	"errors"
	"sync"
	"testing"

	dbm "github.com/cosmos/iavl/db"
	"github.com/stretchr/testify/require"
)

func TestExportSetRace(t *testing.T) {
	tree := NewMutableTree(dbm.NewMemDB(), 0, false, NewNopLogger())

	const count = 5000
	for i := 0; i < count; i++ {
		k := make([]byte, 8)
		binary.BigEndian.PutUint64(k, uint64(i))
		_, err := tree.Set(k, k)
		require.NoError(t, err)
	}
	_, _, err := tree.SaveVersion()
	require.NoError(t, err)

	snapshot := tree.ImmutableTree.clone()
	start := make(chan struct{})
	errCh := make(chan error, 2)
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start

		exporter, err := snapshot.Export()
		if err != nil {
			errCh <- err
			return
		}
		defer exporter.Close()

		for {
			_, err = exporter.Next()
			if err == nil {
				continue
			}
			if errors.Is(err, ErrorExportDone) {
				return
			}

			errCh <- err
			return
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start

		for i := 0; i < count; i++ {
			k := make([]byte, 8)
			binary.BigEndian.PutUint64(k, uint64(i))
			if _, err := tree.Set(k, []byte("x")); err != nil {
				errCh <- err
				return
			}
		}

		if _, _, err := tree.SaveVersion(); err != nil {
			errCh <- err
		}
	}()

	close(start)
	wg.Wait()
	close(errCh)

	for err := range errCh {
		require.NoError(t, err)
	}
}
