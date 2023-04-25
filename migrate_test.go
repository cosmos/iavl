package iavl

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"testing"

	"cosmossdk.io/log"
	dbm "github.com/cosmos/cosmos-db"
	"github.com/stretchr/testify/require"
)

func createLegacyTree(t *testing.T, dbType, dbDir string, version int) error {
	cmd := exec.Command("sh", "-c", fmt.Sprintf("cd cmd/dbgenerator && go run main.go %s %s random %d", dbType, dbDir, version)) //nolint:gosec
	var out bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		t.Log(fmt.Sprint(err) + ": " + stderr.String())
	}
	t.Log("Result: " + out.String())

	return err
}

func TestLazySet(t *testing.T) {
	legacyVersion := 1000
	dbType := "goleveldb"
	dbDir := fmt.Sprintf("legacy-%s-%d", dbType, legacyVersion)
	relateDir := fmt.Sprintf("./cmd/dbgenerator/%s", dbDir)

	defer func() {
		err := os.RemoveAll(relateDir)
		if err != nil {
			t.Errorf("%+v\n", err)
		}
	}()

	require.NoError(t, createLegacyTree(t, dbType, dbDir, legacyVersion))
	db, err := dbm.NewDB("test", dbm.GoLevelDBBackend, relateDir)
	require.NoError(t, err)

	tree, err := NewMutableTree(db, 1000, false, log.NewNopLogger())
	require.NoError(t, err)

	// Load the latest legacy version
	_, err = tree.LoadVersion(int64(legacyVersion))
	require.NoError(t, err)

	// Commit new versions
	postVersions := 1000
	for i := 0; i < postVersions; i++ {
		leafCount := rand.Intn(50)
		for j := 0; j < leafCount; j++ {
			_, err = tree.Set([]byte(fmt.Sprintf("key-%d-%d", i, j)), []byte(fmt.Sprintf("value-%d-%d", i, j)))
			require.NoError(t, err)
		}
		_, _, err = tree.SaveVersion()
		require.NoError(t, err)
	}

	tree, err = NewMutableTree(db, 1000, false, log.NewNopLogger())
	require.NoError(t, err)

	// Verify that the latest legacy version can still be loaded
	_, err = tree.LoadVersion(int64(legacyVersion))
	require.NoError(t, err)
}

func TestDeleteVersions(t *testing.T) {
	legacyVersion := 100
	dbType := "goleveldb"
	dbDir := fmt.Sprintf("./legacy-%s-%d", dbType, legacyVersion)
	relateDir := fmt.Sprintf("./cmd/dbgenerator/%s", dbDir)

	defer func() {
		err := os.RemoveAll(relateDir)
		if err != nil {
			t.Errorf("%+v\n", err)
		}
	}()

	require.NoError(t, createLegacyTree(t, dbType, dbDir, legacyVersion))
	db, err := dbm.NewDB("test", dbm.GoLevelDBBackend, relateDir)
	require.NoError(t, err)

	tree, err := NewMutableTree(db, 1000, false, log.NewNopLogger())
	require.NoError(t, err)

	// Load the latest legacy version
	_, err = tree.LoadVersion(int64(legacyVersion))
	require.NoError(t, err)

	// Commit new versions
	postVersions := 100
	for i := 0; i < postVersions; i++ {
		leafCount := rand.Intn(10)
		for j := 0; j < leafCount; j++ {
			_, err = tree.Set([]byte(fmt.Sprintf("key-%d-%d", i, j)), []byte(fmt.Sprintf("value-%d-%d", i, j)))
			require.NoError(t, err)
		}
		_, _, err = tree.SaveVersion()
		require.NoError(t, err)
	}

	// Check the available versions
	versions := tree.AvailableVersions()
	targetVersion := 0
	for i := len(versions) - 1; i >= 0; i-- {
		if versions[i] < legacyVersion {
			targetVersion = versions[i]
			break
		}
	}

	// Test LoadVersionForOverwriting for the legacy version
	err = tree.LoadVersionForOverwriting(int64(targetVersion))
	require.NoError(t, err)
	latestVersion, err := tree.ndb.getLatestVersion()
	require.NoError(t, err)
	require.Equal(t, int64(targetVersion), latestVersion)
	legacyLatestVersion, err := tree.ndb.getLegacyLatestVersion()
	require.NoError(t, err)
	require.Equal(t, int64(targetVersion), legacyLatestVersion)

	// Test DeleteVersionsTo for the legacy version
	for i := 0; i < postVersions; i++ {
		leafCount := rand.Intn(20)
		for j := 0; j < leafCount; j++ {
			_, err = tree.Set([]byte(fmt.Sprintf("key-%d-%d", i, j)), []byte(fmt.Sprintf("value-%d-%d", i, j)))
			require.NoError(t, err)
		}
		_, _, err = tree.SaveVersion()
		require.NoError(t, err)
	}
	// Check if the legacy versions are deleted at once
	versions = tree.AvailableVersions()
	err = tree.DeleteVersionsTo(legacyLatestVersion - 1)
	require.NoError(t, err)
	pVersions := tree.AvailableVersions()
	require.Equal(t, len(versions), len(pVersions))
	toVersion := legacyLatestVersion + int64(postVersions)/2
	err = tree.DeleteVersionsTo(toVersion)
	require.NoError(t, err)
	pVersions = tree.AvailableVersions()
	require.Equal(t, postVersions/2, len(pVersions))
}
