package testutil

import (
	"fmt"

	"github.com/cosmos/iavl-bench/bench"
	"github.com/dustin/go-humanize"
	"github.com/kocubinski/costor-api/compact"
	"github.com/kocubinski/costor-api/logz"
)

type TreeBuildOptions struct {
	Until       int64
	UntilHash   string
	LoadVersion int64
	Iterator    bench.ChangesetIterator
	Report      func()
	SampleRate  int64
}

func (opts *TreeBuildOptions) With10_000() *TreeBuildOptions {
	opts.Until = 10_000
	opts.UntilHash = "34d9a0d607ecd96ddbde4c0089ee4be633bf0b73b9b6c9da827f7305f1591044"
	return opts
}

func (opts *TreeBuildOptions) With25_000() *TreeBuildOptions {
	opts.Until = 25_000
	opts.UntilHash = "08482db3715bef1f3251894b8c37901950a4787ac5f10d75ab4318e4fea91642"
	return opts
}

func (opts *TreeBuildOptions) FastForward(version int64) error {
	log := logz.Logger.With().Str("module", "fastForward").Logger()
	log.Info().Msgf("fast forwarding changesets to version %d...", opts.LoadVersion+1)
	i := 1
	itr := opts.Iterator
	var err error
	for ; itr.Valid(); err = itr.Next() {
		if itr.Version() > version {
			break
		}
		if err != nil {
			return err
		}
		nodes := itr.Nodes()
		for ; nodes.Valid(); err = nodes.Next() {
			if err != nil {
				return err
			}
			if i%5_000_000 == 0 {
				fmt.Printf("fast forward %s nodes\n", humanize.Comma(int64(i)))
			}
			i++
		}
	}
	log.Info().Msgf("fast forward complete")
	return nil
}

func NewTreeBuildOptions() *TreeBuildOptions {
	var seed int64 = 1234
	var versions int64 = 10_000_000
	bankGen := bench.BankLikeGenerator(seed, versions)
	// bankGen.InitialSize = 10_000
	lockupGen := bench.LockupLikeGenerator(seed, versions)
	// lockupGen.InitialSize = 10_000
	stakingGen := bench.StakingLikeGenerator(seed, versions)
	// stakingGen.InitialSize = 10_000
	itr, err := bench.NewChangesetIterators([]bench.ChangesetGenerator{
		bankGen,
		lockupGen,
		stakingGen,
	})
	if err != nil {
		panic(err)
	}
	opts := TreeBuildOptions{
		Iterator: itr,
	}
	return opts.With25_000()
}

func BankLockup25_000() TreeBuildOptions {
	var seed int64 = 1234
	var versions int64 = 10_000_000
	bankGen := bench.BankLikeGenerator(seed, versions)
	lockupGen := bench.LockupLikeGenerator(seed, versions)
	itr, err := bench.NewChangesetIterators([]bench.ChangesetGenerator{
		bankGen,
		lockupGen,
	})
	if err != nil {
		panic(err)
	}
	opts := TreeBuildOptions{
		Iterator:  itr,
		Until:     25_000,
		UntilHash: "c1dc9dc7d3a8ae025d2a347eea19121e98435b06b421607119bc3cf3cf79be05",
	}
	return opts
}

func BigTreeOptions100_000() *TreeBuildOptions {
	var seed int64 = 1234
	var versions int64 = 200_000
	bankGen := bench.BankLikeGenerator(seed, versions)
	lockupGen := bench.LockupLikeGenerator(seed, versions)
	stakingGen := bench.StakingLikeGenerator(seed, versions)
	itr, err := bench.NewChangesetIterators([]bench.ChangesetGenerator{
		bankGen,
		lockupGen,
		stakingGen,
	})
	if err != nil {
		panic(err)
	}
	opts := &TreeBuildOptions{
		Iterator:  itr,
		Until:     100,
		UntilHash: "c1dc9dc7d3a8ae025d2a347eea19121e98435b06b421607119bc3cf3cf79be05",
	}
	return opts
}

func BigStartOptions() TreeBuildOptions {
	initialSize := 1_000_000
	var seed int64 = 1234
	var versions int64 = 10_000
	bankGen := bench.BankLikeGenerator(seed, versions)
	bankGen.InitialSize = initialSize
	lockupGen := bench.LockupLikeGenerator(seed, versions)
	lockupGen.InitialSize = initialSize
	stakingGen := bench.StakingLikeGenerator(seed, versions)
	stakingGen.InitialSize = initialSize

	itr, err := bench.NewChangesetIterators([]bench.ChangesetGenerator{
		bankGen,
		lockupGen,
		stakingGen,
	})
	if err != nil {
		panic(err)
	}

	opts := TreeBuildOptions{
		Iterator:  itr,
		Until:     300,
		UntilHash: "b7266b2b30979e1415bcb8ef7fed9637b542213fefd1bb77374aa1f14442aa50", // 300
	}

	return opts
}

func OsmoLike() *TreeBuildOptions {
	initialSize := 20_000_000 // revert to 20M!!
	finalSize := int(1.5 * float64(initialSize))
	var seed int64 = 1234
	var versions int64 = 1_000_000
	bankGen := bench.BankLikeGenerator(seed, versions)
	bankGen.InitialSize = initialSize
	bankGen.FinalSize = finalSize
	bankGen2 := bench.BankLikeGenerator(seed+1, versions)
	bankGen2.InitialSize = initialSize
	bankGen2.FinalSize = finalSize
	// lockupGen := bench.LockupLikeGenerator(seed, versions)
	// lockupGen.InitialSize = initialSize
	// stakingGen := bench.StakingLikeGenerator(seed, versions)
	// stakingGen.InitialSize = initialSize

	itr, err := bench.NewChangesetIterators([]bench.ChangesetGenerator{
		bankGen,
		bankGen2,
	})
	if err != nil {
		panic(err)
	}

	opts := &TreeBuildOptions{
		Iterator: itr,
		Until:    10_000,
		// hash for 10k WITHOUT a store key prefix on the key
		UntilHash: "e996df6099bc4b6e8a723dc551af4fa7cfab50e3a182ab1e21f5e90e5e7124cd", // 10000
		// hash for 10k WITH store key prefix on key
		// UntilHash: "3b43ef49895a7c483ef4b9a84a1f0ddbe7615c9a65bc533f69bc6bf3eb1b3d6c", // OsmoLike, 10000
	}

	return opts
}

func OsmoLikeManyTrees() *TreeBuildOptions {
	seed := int64(1234)
	versions := int64(100_000)
	changes := int(versions / 100)
	deleteFrac := 0.2

	wasm := bench.ChangesetGenerator{
		StoreKey:         "wasm",
		Seed:             seed,
		KeyMean:          79,
		KeyStdDev:        23,
		ValueMean:        170,
		ValueStdDev:      202,
		InitialSize:      8_500_000,
		FinalSize:        8_600_000,
		Versions:         versions,
		ChangePerVersion: changes,
		DeleteFraction:   deleteFrac,
	}
	ibc := bench.ChangesetGenerator{
		StoreKey:         "ibc",
		Seed:             seed,
		KeyMean:          58,
		KeyStdDev:        4,
		ValueMean:        22,
		ValueStdDev:      29,
		InitialSize:      23_400_000,
		FinalSize:        23_500_000,
		Versions:         versions,
		ChangePerVersion: changes,
		DeleteFraction:   deleteFrac,
	}
	upgrade := bench.ChangesetGenerator{
		StoreKey:         "upgrade",
		Seed:             seed,
		KeyMean:          20,
		KeyStdDev:        1,
		ValueMean:        8,
		ValueStdDev:      0,
		InitialSize:      60,
		FinalSize:        62,
		Versions:         versions,
		ChangePerVersion: 1,
		DeleteFraction:   0,
	}
	concentratedliquidity := bench.ChangesetGenerator{
		StoreKey:         "concentratedliquidity",
		Seed:             seed,
		KeyMean:          25,
		KeyStdDev:        11,
		ValueMean:        44,
		ValueStdDev:      48,
		InitialSize:      600_000,
		FinalSize:        610_000,
		Versions:         versions,
		ChangePerVersion: changes,
		DeleteFraction:   deleteFrac,
	}
	icahost := bench.ChangesetGenerator{
		StoreKey:         "icahost",
		Seed:             seed,
		KeyMean:          103,
		KeyStdDev:        11,
		ValueMean:        37,
		ValueStdDev:      25,
		InitialSize:      1_500,
		FinalSize:        1_600,
		Versions:         versions,
		ChangePerVersion: changes,
		DeleteFraction:   deleteFrac,
	}
	capability := bench.ChangesetGenerator{
		StoreKey:         "capability",
		Seed:             seed,
		KeyMean:          24,
		KeyStdDev:        1,
		ValueMean:        42,
		ValueStdDev:      1,
		InitialSize:      5_000,
		FinalSize:        5_400,
		Versions:         versions,
		ChangePerVersion: changes,
		DeleteFraction:   deleteFrac,
	}
	authz := bench.ChangesetGenerator{
		StoreKey:         "authz",
		Seed:             seed,
		KeyMean:          83,
		KeyStdDev:        9,
		ValueMean:        113,
		ValueStdDev:      30,
		InitialSize:      45_000,
		FinalSize:        48_000,
		Versions:         versions,
		ChangePerVersion: changes,
		DeleteFraction:   deleteFrac,
	}
	incentives := bench.ChangesetGenerator{
		StoreKey:         "incentives",
		Seed:             seed,
		KeyMean:          23,
		KeyStdDev:        5,
		ValueMean:        91,
		ValueStdDev:      20,
		InitialSize:      45_000,
		FinalSize:        50_000,
		Versions:         versions,
		ChangePerVersion: changes,
		DeleteFraction:   deleteFrac,
	}
	acc := bench.ChangesetGenerator{
		StoreKey:         "acc",
		Seed:             seed,
		KeyMean:          21,
		KeyStdDev:        1,
		ValueMean:        149,
		ValueStdDev:      25,
		InitialSize:      850_000,
		FinalSize:        940_000,
		Versions:         versions,
		ChangePerVersion: changes,
		DeleteFraction:   deleteFrac,
	}
	superfluid := bench.ChangesetGenerator{
		StoreKey:         "superfluid",
		Seed:             seed,
		KeyMean:          10,
		KeyStdDev:        2,
		ValueMean:        22,
		ValueStdDev:      11,
		InitialSize:      850_000,
		FinalSize:        940_000,
		Versions:         versions,
		ChangePerVersion: changes,
		DeleteFraction:   deleteFrac,
	}
	slashing := bench.ChangesetGenerator{
		StoreKey:         "slashing",
		Seed:             seed,
		KeyMean:          30,
		KeyStdDev:        1,
		ValueMean:        2,
		ValueStdDev:      1,
		InitialSize:      3_100_000,
		FinalSize:        3_700_000,
		Versions:         versions,
		ChangePerVersion: changes,
		DeleteFraction:   deleteFrac,
	}
	gov := bench.ChangesetGenerator{
		StoreKey:         "gov",
		Seed:             seed,
		KeyMean:          28,
		KeyStdDev:        5,
		ValueMean:        600,
		ValueStdDev:      7200,
		InitialSize:      7_000,
		FinalSize:        7_500,
		Versions:         versions,
		ChangePerVersion: changes,
		DeleteFraction:   deleteFrac,
	}
	distribution := bench.ChangesetGenerator{
		StoreKey:         "distribution",
		Seed:             seed,
		KeyMean:          36,
		KeyStdDev:        6,
		ValueMean:        158,
		ValueStdDev:      150,
		InitialSize:      2_600_000,
		FinalSize:        3_300_000,
		Versions:         versions,
		ChangePerVersion: changes,
		DeleteFraction:   deleteFrac,
	}
	twap := bench.ChangesetGenerator{
		StoreKey:         "twap",
		Seed:             seed,
		KeyMean:          157,
		KeyStdDev:        31,
		ValueMean:        272,
		ValueStdDev:      31,
		InitialSize:      400_000,
		FinalSize:        480_000,
		Versions:         versions,
		ChangePerVersion: changes,
		DeleteFraction:   deleteFrac,
	}
	staking := bench.ChangesetGenerator{
		StoreKey:         "staking",
		Seed:             seed,
		KeyMean:          42,
		KeyStdDev:        2,
		ValueMean:        505,
		ValueStdDev:      4950,
		InitialSize:      1_500_000,
		FinalSize:        1_700_000,
		Versions:         versions,
		ChangePerVersion: changes,
		DeleteFraction:   deleteFrac,
	}
	bank := bench.ChangesetGenerator{
		StoreKey:         "bank",
		Seed:             seed,
		KeyMean:          57,
		KeyStdDev:        25,
		ValueMean:        45,
		ValueStdDev:      25,
		InitialSize:      2_000_000,
		FinalSize:        2_300_000,
		Versions:         versions,
		ChangePerVersion: changes,
		DeleteFraction:   deleteFrac,
	}
	lockup := bench.ChangesetGenerator{
		StoreKey:         "lockup",
		Seed:             seed,
		KeyMean:          60,
		KeyStdDev:        35,
		ValueMean:        25,
		ValueStdDev:      36,
		InitialSize:      2_000_000,
		FinalSize:        2_300_000,
		Versions:         versions,
		ChangePerVersion: changes,
		DeleteFraction:   deleteFrac,
	}

	itr, err := bench.NewChangesetIterators([]bench.ChangesetGenerator{
		wasm,
		ibc,
		upgrade,
		concentratedliquidity,
		icahost,
		capability,
		authz,
		incentives,
		acc,
		superfluid,
		slashing,
		gov,
		distribution,
		twap,
		staking,
		bank,
		lockup,
	})
	if err != nil {
		panic(err)
	}
	return &TreeBuildOptions{
		Iterator: itr,
		Until:    versions,
	}
}

func CompactedChangelogs(logDir string) *TreeBuildOptions {
	itr, err := compact.NewChangesetIterator(logDir)
	if err != nil {
		panic(err)
	}
	return &TreeBuildOptions{
		Iterator: itr,
		Until:    10_000,
		// hash for 10k WITHOUT a store key prefix on the key
		UntilHash: "e996df6099bc4b6e8a723dc551af4fa7cfab50e3a182ab1e21f5e90e5e7124cd", // 10000
		// hash for 10k WITH store key prefix on key
		// UntilHash: "3b43ef49895a7c483ef4b9a84a1f0ddbe7615c9a65bc533f69bc6bf3eb1b3d6c", // OsmoLike, 10000
	}
}
