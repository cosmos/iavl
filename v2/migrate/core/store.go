package core

import (
	"fmt"
	"strings"
	"sync"
	"time"

	store "cosmossdk.io/api/cosmos/store/v1beta1"
	db "github.com/cosmos/cosmos-db"
	"github.com/cosmos/iavl"
	gogotypes "github.com/gogo/protobuf/types"
	"github.com/kocubinski/costor-api/logz"
	"github.com/pkg/errors"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"google.golang.org/protobuf/proto"
)

const (
	CommitInfoKeyFmt = "s/%d" // s/<version>
	LatestVersionKey = "s/latest"
)

var (
	log       = logz.Logger.With().Str("module", "store").Logger()
	dbOpenMtx sync.Mutex
)

type ReadonlyStore struct {
	db.DB
	commitInfoByName map[string]*store.CommitInfo
}

func (rs *ReadonlyStore) CommitInfoByName() map[string]*store.CommitInfo {
	return rs.commitInfoByName
}

func (rs *ReadonlyStore) getCommitInfoFromDB(ver int64) (*store.CommitInfo, error) {
	cInfoKey := fmt.Sprintf(CommitInfoKeyFmt, ver)

	bz, err := rs.DB.Get([]byte(cInfoKey))
	if err != nil {
		return nil, errors.Wrap(err, "failed to get commit info")
	} else if bz == nil {
		return nil, errors.New("no commit info found")
	}

	cInfo := &store.CommitInfo{}
	if err = proto.Unmarshal(bz, cInfo); err != nil {
		return nil, errors.Wrap(err, "failed unmarshal commit info")
	}

	return cInfo, nil
}

func NewReadonlyStore(dbPath string) (*ReadonlyStore, error) {
	l := log.With().
		Str("dbPath", fmt.Sprintf("%s/application.db", dbPath)).
		Str("op", "NewReadonlyStore").
		Logger()
	since := time.Now()
	l.Info().Msg("waiting for lock")
	dbOpenMtx.Lock()
	l.Info().Msgf("got lock in %s", time.Since(since))
	defer dbOpenMtx.Unlock()

	var err error
	rs := &ReadonlyStore{
		commitInfoByName: make(map[string]*store.CommitInfo),
	}
	since = time.Now()
	rs.DB, err = db.NewGoLevelDBWithOpts("application", dbPath, &opt.Options{
		ReadOnly: true,
	})
	l.Info().Msgf("opened in %s", time.Since(since))
	if err != nil {
		return nil, err
	}
	latestVersionBz, err := rs.DB.Get([]byte(LatestVersionKey))
	if err != nil {
		return nil, err
	}

	var latestVersion int64
	if err := gogotypes.StdInt64Unmarshal(&latestVersion, latestVersionBz); err != nil {
		return nil, err
	}

	since = time.Now()
	commitInfo, err := rs.getCommitInfoFromDB(latestVersion)
	if err != nil {
		return nil, err
	}

	var storeInfoNames []string
	for _, si := range commitInfo.StoreInfos {
		storeInfoNames = append(storeInfoNames, si.Name)
		rs.commitInfoByName[si.Name] = commitInfo
	}
	log.Info().Msgf("loaded commit info in %s for stores %s", time.Since(since),
		strings.Join(storeInfoNames, " "))

	return rs, nil
}

var ErrStoreNotFound = errors.New("store not found")

func (rs *ReadonlyStore) LatestTree(storeKey string) (db.DB, *iavl.MutableTree, error) {
	//since := time.Now()

	prefix := fmt.Sprintf("s/k:%s/", storeKey)
	prefixDb := db.NewPrefixDB(rs.DB, []byte(prefix))
	tree, err := iavl.NewMutableTreeWithOpts(prefixDb, 1000, &iavl.Options{InitialVersion: 0}, true)
	if err != nil {
		return nil, nil, err
	}
	commitInfo, ok := rs.commitInfoByName[storeKey]
	if !ok {
		return nil, nil, ErrStoreNotFound
	}
	_, err = tree.LoadVersion(commitInfo.Version)
	if err != nil {
		return nil, nil, err
	}

	//log.Info().Msgf("loaded tree in %s", time.Since(since))

	return prefixDb, tree, nil
}
