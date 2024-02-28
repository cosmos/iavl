package iavl

import (
	"fmt"
)

type VersionRange struct {
	versions []int64
	cache    map[int64]int64
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

// Find returns the shard that contains the given version by binary searching
// the version range. If the version is after the last shard, -1 is returned.
func (r *VersionRange) Find(version int64) int64 {
	vs := r.versions
	if len(vs) == 0 || version > vs[len(vs)-1] {
		return -1
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
	return vs[low]
}

func (r *VersionRange) FindPrevious(version int64) int64 {
	vs := r.versions
	if len(vs) == 0 || version < vs[0] {
		return -1
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

func (r *VersionRange) FindMemoized(version int64) int64 {
	if r.cache == nil {
		r.cache = make(map[int64]int64)
	}
	if v, ok := r.cache[version]; ok {
		return v
	}
	v := r.Find(version)
	// don't cache err values
	if v == -1 {
		return -1
	}
	r.cache[version] = v
	return v
}

func (r *VersionRange) Last() int64 {
	if len(r.versions) == 0 {
		return -1
	}
	return r.versions[len(r.versions)-1]
}

func (r *VersionRange) Len() int {
	return len(r.versions)
}
