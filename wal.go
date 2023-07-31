package iavl

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	dbm "github.com/cosmos/cosmos-db"
	"github.com/tidwall/wal"
)

type CountMetric interface {
	Inc()
}

type GaugeMetric interface {
	Add(float64)
	Sub(float64)
	Set(float64)
}

type NaiveWal struct {
	logDir string
}

func (w *NaiveWal) filename(index int64) string {
	return fmt.Sprintf("%s/%012d.wal", w.logDir, index)
}

func (w *NaiveWal) index(filename string) (int64, error) {
	var index int64
	parts := strings.Split(filename, "/")
	last := parts[len(parts)-1]
	_, err := fmt.Sscanf(last, "%012d.wal", &index)
	if err != nil {
		return 0, err
	}
	return index, nil
}

func (w *NaiveWal) Write(index int64, bz []byte) error {
	fn := w.filename(index)
	f, err := os.Create(fn)
	if err != nil {
		return err
	}
	_, err = f.Write(bz)
	if err != nil {
		return err
	}
	return f.Close()
}

func (w *NaiveWal) Open(index int64) (io.Reader, error) {
	fn := w.filename(index)
	return os.Open(fn)
}

func (w *NaiveWal) TakeN(n int) ([]os.DirEntry, error) {
	var res []os.DirEntry
	entries, err := os.ReadDir(w.logDir)
	if err != nil {
		return res, err
	}
	for i, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if i > n {
			return res, nil
		}
		res = append(res, entry)
	}
	return res, nil
}

func (w *NaiveWal) Remove(index int64) error {
	return os.Remove(w.filename(index))
}

func NewNaiveWal(logDir string) (*NaiveWal, error) {
	_, err := os.Stat(logDir)
	if os.IsNotExist(err) {
		err = os.Mkdir(logDir, 0755)
		if err != nil {
			return nil, err
		}
	}
	return &NaiveWal{logDir: logDir}, nil
}

func NewTidwalLog(logDir string) (*wal.Log, error) {
	walOpts := wal.DefaultOptions
	walOpts.NoSync = true
	walOpts.NoCopy = true
	log, err := wal.Open(fmt.Sprintf("%s/iavl.wal", logDir), walOpts)
	return log, err
}

type Wal struct {
	wal                *NaiveWal
	tidwall            *wal.Log
	commitment         dbm.DB
	storage            *SqliteDb
	checkpointInterval int

	cache     map[nodeCacheKey]*deferredNode
	cacheLock sync.RWMutex

	MetricNodesRead CountMetric
	MetricWalSize   GaugeMetric
	MetricCacheMiss CountMetric
	MetricCacheHit  CountMetric
	MetricCacheSize GaugeMetric
}

func NewWal(wal *wal.Log, commitment dbm.DB, storage *SqliteDb) *Wal {
	return &Wal{
		tidwall:            wal,
		commitment:         commitment,
		storage:            storage,
		cache:              make(map[nodeCacheKey]*deferredNode),
		checkpointInterval: 30,
	}
}

func (r *Wal) Write(idx uint64, bz []byte) error {
	if r.MetricWalSize != nil {
		r.MetricWalSize.Add(float64(len(bz)))
	}
	return r.tidwall.Write(idx, bz)
}

func (r *Wal) CacheGet(key nodeCacheKey) (*Node, error) {
	r.cacheLock.RLock()
	defer r.cacheLock.RUnlock()

	if dn, ok := r.cache[key]; ok {
		node, err := MakeNode(key[:], dn.nodeBz)
		if err != nil {
			return nil, err
		}
		if r.MetricCacheHit != nil {
			r.MetricCacheHit.Inc()
		}
		return node, nil
	}

	if r.MetricCacheMiss != nil {
		r.MetricCacheMiss.Inc()
	}
	return nil, nil
}

func (r *Wal) FirstIndex() (uint64, error) {
	return r.tidwall.FirstIndex()
}

func (r *Wal) RunNaive(ctxt context.Context) error {
	for {
		select {
		case <-ctxt.Done():
			return ctxt.Err()
		default:
			entries, err := r.wal.TakeN(100)
			if err != nil {
				return err
			}
			if len(entries) == 0 {
				continue
			}
			for _, entry := range entries {
				changeset := &ChangeSet{}
				version, err := r.wal.index(entry.Name())
				if err != nil {
					return err
				}
				f, err := os.Open(r.wal.filename(version))
				if err != nil {
					return err
				}
				// TODO use reader once settled on WAL
				bz, err := io.ReadAll(f)
				for i := 0; i < len(bz); {
					deleted := bz[i] == 1
					i += 1
					nodeKey := bz[i : i+12]
					i += 12
					size := binary.BigEndian.Uint32(bz[i : i+4])
					i += 4
					nodeBz := bz[i : i+int(size)]
					i += int(size)
					if !deleted {
						err = r.commitment.Set(nodeKey, nodeBz)
						if err != nil {
							return err
						}
					} else {
						err = r.commitment.Delete(nodeKey)
						if err != nil {
							return err
						}
					}

					node, err := MakeNode(nodeKey, nodeBz)
					if err != nil {
						return err
					}
					changeset.Pairs = append(changeset.Pairs, &KVPair{Key: node.key, Value: node.value, Delete: deleted})
				}

				if err := r.wal.Remove(version); err != nil {
					return err
				}
			}

			// persistent storage
			//if err := r.storage.StoreChangeSet(int64(v), changeset); err != nil {
			//	return err
			//}

			// remove from wal
		}
	}
}

type deferredNode struct {
	nodeBz  []byte
	deleted bool
}

func (r *Wal) Run(ctxt context.Context) error {
	var (
		checkpointBz   float64
		checkpointHead uint64
		index          uint64
		err            error
	)

	for {
		select {
		case <-ctxt.Done():
			return ctxt.Err()
		default:
			// clean start on first index
			if index == 0 {
				index, err = r.tidwall.FirstIndex()
				if err != nil {
					return err
				}
				if index == 0 {
					continue
				}
			}

			// advancing index
			bz, err := r.tidwall.Read(index)
			if err != nil {
				if errors.Is(err, wal.ErrNotFound) {
					continue
				}
			}

			if checkpointHead == 0 {
				checkpointHead = index
			}

			for i := 0; i < len(bz); {
				deleted := bz[i] == 1
				i += 1
				nodeKey := bz[i : i+12]
				i += 12
				size := binary.BigEndian.Uint32(bz[i : i+4])
				i += 4
				nodeBz := bz[i : i+int(size)]
				i += int(size)

				var nk nodeCacheKey
				copy(nk[:], nodeKey)
				r.cacheLock.Lock()
				if !deleted {
					r.cache[nk] = &deferredNode{nodeBz: nodeBz, deleted: false}
					if r.MetricCacheSize != nil {
						r.MetricCacheSize.Add(1)
					}
				} else {
					if _, ok := r.cache[nk]; !ok {
						// deleting a key that is persisted
						r.cache[nk] = &deferredNode{nodeBz: nodeBz, deleted: true}
						if r.MetricCacheSize != nil {
							r.MetricCacheSize.Add(1)
						}
					} else {
						// deleting a key that is not persisted; never makes it to disk
						delete(r.cache, nk)
						if r.MetricCacheSize != nil {
							r.MetricCacheSize.Sub(1)
						}
					}
				}
				r.cacheLock.Unlock()

				if r.MetricNodesRead != nil {
					r.MetricNodesRead.Inc()
				}
			}
			checkpointBz += float64(len(bz))
			index++

			if index-checkpointHead >= uint64(r.checkpointInterval) {
				fmt.Printf("wal: checkpointing now. [%d - %d) will be flushed to state commitment\n",
					checkpointHead, index)
				for k, dn := range r.cache {
					if !dn.deleted {
						err = r.commitment.Set(k[:], dn.nodeBz)
						if err != nil {
							return err
						}
					} else {
						err = r.commitment.Delete(k[:])
						if err != nil {
							return err
						}
					}
				}
				if err := r.tidwall.TruncateFront(index); err != nil {
					return err
				}

				r.cacheLock.Lock()
				r.cache = make(map[nodeCacheKey]*deferredNode)
				r.cacheLock.Unlock()

				if r.MetricWalSize != nil {
					r.MetricWalSize.Sub(checkpointBz)
				}
				if r.MetricCacheSize != nil {
					r.MetricCacheSize.Set(0)
				}
				checkpointHead = 0
				checkpointBz = 0
			}
		}

		// persistent storage
		//if err := r.storage.StoreChangeSet(int64(v), changeset); err != nil {
		//	return err
		//}

		// remove from wal
	}
}
