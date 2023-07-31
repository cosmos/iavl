package zorder

func getInt64Bit(n int64, i int) byte {
	return byte((n >> i) & 0x01)
}

func getBytesBit(b []byte, i int) byte {
	return byte((b[i/8] >> (i % 8)) & 0x01)
}

func Interleave(key []byte, version int64) []byte {
	keyBitLength := len(key) * 8
	zBitLength := keyBitLength + 64
	z := make([]byte, len(key)+8)

	max := len(key) * 8
	if len(key) < 64 {
		max = 64
	}

	for zi := 0; zi < zBitLength; {
		for i := 0; i < max; i++ {
			if i < keyBitLength {
				bit := getBytesBit(key, i)
				z[zi/8] |= bit << (zi % 8)
				zi++
			}
			if i < 64 {
				bit := getInt64Bit(version, i)
				z[zi/8] |= bit << (zi % 8)
				zi++
			}
		}
	}
	return z
}
