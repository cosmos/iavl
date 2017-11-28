package iavl

// KeyValue groups together a key and a value for return codes
type KeyValue struct {
	Key   []byte
	Value []byte
}

// SerializeFunc is any implementation that can serialize
// an iavl Tree
type SerializeFunc func(*Tree) []KeyValue

// Restore will take an (empty) tree restore it
// from the keys returned from a SerializeFunc
func Restore(empty *Tree, kvs []KeyValue) {
	for _, kv := range kvs {
		empty.Set(kv.Key, kv.Value)
	}
	empty.Hash()
}

// InOrderSerializer returns all key-values in the
// key order (as stored). May be nice to read, but
// when recovering, it will create a different.
func InOrderSerializer(tree *Tree) []KeyValue {
	res := make([]KeyValue, 0, tree.Size())
	tree.Iterate(func(key, value []byte) bool {
		kv := KeyValue{Key: key, Value: value}
		res = append(res, kv)
		return false
	})
	return res
}
