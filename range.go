package iavl

import (
	"fmt"

	"github.com/bvinc/go-sqlite-lite/sqlite3"
)

type VersionRange struct {
	versions []int64
	queries  map[int64]*sqlite3.Stmt
}

func (r *VersionRange) Add(version int64) error {
	if len(r.versions) == 0 {
		r.versions = append(r.versions, version)
		return nil
	}
	if version <= r.versions[len(r.versions)-1] {
		return fmt.Errorf("unordered insert: version %d is not greater than %d", version, r.versions[len(r.versions)-1])
	}
	if version == r.versions[len(r.versions)-1] {
		return fmt.Errorf("duplicate version: %d", version)
	}
	r.versions = append(r.versions, version)
	return nil
}

// Find returns the version of the shard that contains the given version by binary searching
// the version range. If the version is before the first shard, -1 is returned.
func (r *VersionRange) Find(version int64) int64 {
	vs := r.versions
	if len(vs) == 0 || version < vs[0] {
		return -1
	}
	if version > vs[len(vs)-1] {
		return vs[len(vs)-1]
	}
	low, high := 0, len(vs)-1
	for low <= high {
		mid := (low + high) / 2
		if vs[mid] == version {
			return version
		}
		if vs[mid] < version {
			low = mid + 1
		} else {
			high = mid - 1
		}
	}
	return vs[high]
}
