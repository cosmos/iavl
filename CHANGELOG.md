# Changelog

## 0.9.2 (July 3, 2018)

IMPROVEMTS

- some minor changes: mainly lints, updated parts of documentation, unexported some helpers (#80) 

## 0.9.1 (July 1, 2018)

IMPROVEMENTS

- RangeProof.ComputeRootHash() to compute root rather than provide as in Verify(hash)
- RangeProof.Verify\*() first require .Verify(root), which memoizes

## 0.9.0 (July 1, 2018)

BREAKING CHANGES

- RangeProof.VerifyItem doesn't require an index.
- Only return values in range when getting proof.
- Return keys as well.

BUG FIXES

- traversal bugs in traverseRange.

## 0.8.2

* Swap `tmlibs` for `tendermint/libs`
* Remove `sha256truncated` in favour of `tendermint/crypto/tmhash` - same hash
  function but technically a breaking change to the API, though unlikely to effect anyone.

NOTE this means IAVL is now dependent on Tendermint Core for the libs (since it
makes heavy use of the `db` package). Ideally, that dependency would be
abstracted away, and/or this repo will be merged into the Cosmos-SDK, which is
currently is primary consumer. Once it achieves greater stability, we could
consider breaking it out into it's own repo again.

## 0.8.1

*July 1st, 2018*

BUG FIXES

- fix bug in iterator going outside its range

## 0.8.0 (June 24, 2018)

BREAKING CHANGES

- Nodes are encoded using proto3/amino style integers and byte slices (ie. varints and
  varint prefixed byte slices)
- Unified RangeProof
- Proofs are encoded using Amino
- Hash function changed from RIPEMD160 to the first 20 bytes of SHA256 output

## 0.7.0 (March 21, 2018)

BREAKING CHANGES

- LoadVersion and Load return the loaded version number
    - NOTE: this behaviour was lost previously and we failed to document in changelog,
        but now it's back :)

## 0.6.1 (March 2, 2018)

IMPROVEMENT

- Remove spurious print statement from LoadVersion

## 0.6.0 (March 2, 2018)

BREAKING CHANGES

- NewTree order of arguments swapped
- int -> int64, uint64 -> int64
- NewNode takes a version
- Node serialization format changed so version is written right after size
- SaveVersion takes no args (auto increments)
- tree.Get -> tree.Get64
- nodeDB.SaveBranch does not take a callback
- orphaningTree.SaveVersion -> SaveAs
- proofInnerNode includes Version
- ReadKeyXxxProof consolidated into ReadKeyProof
- KeyAbsentProof doesn't include Version
- KeyRangeProof.Version -> Versions

FEATURES

- Implement chunking algorithm to serialize entire tree

## 0.5.0 (October 27, 2017)

First versioned release!
(Originally accidentally released as v0.2.0)

