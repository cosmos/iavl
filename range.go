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

func (r *VersionRange) Remove(version int64) {
	for i, v := range r.versions {
		if v == version {
			r.versions = append(r.versions[:i], r.versions[i+1:]...)
			return
		}
	}
}

func (r *VersionRange) FindNext(version int64) int64 {
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

func (r *VersionRange) FindRecent(version, n int64) int64 {
	v := version
	for {
		prev := r.FindPrevious(v)
		if prev == -1 || prev == r.versions[0] {
			return -1
		}
		if version-prev > n {
			return prev
		}
		v = prev - 1
	}
}

// FindShard returns the shard ID for the given version.
// It calls FindPrevious, but if version < first shardID it returns the first shardID.
func (r *VersionRange) FindShard(version int64) int64 {
	if len(r.versions) == 0 {
		return -1
	}
	v := r.FindPrevious(version)
	if v == -1 {
		v = r.First()
	}
	return v
}

func (r *VersionRange) FindMemoized(version int64) int64 {
	if r.cache == nil {
		r.cache = make(map[int64]int64)
	}
	if v, ok := r.cache[version]; ok {
		return v
	}
	v := r.FindNext(version)
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

func (r *VersionRange) First() int64 {
	if len(r.versions) == 0 {
		return -1
	}
	return r.versions[0]
}

func (r *VersionRange) Len() int {
	return len(r.versions)
}

func (r *VersionRange) Copy() *VersionRange {
	vs := make([]int64, len(r.versions))
	copy(vs, r.versions)
	return &VersionRange{versions: vs, cache: make(map[int64]int64)}
}
