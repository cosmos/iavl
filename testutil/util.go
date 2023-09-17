package testutil

import "github.com/cosmos/iavl-bench/bench"

type TreeBuildOptions struct {
	Until     int64
	UntilHash string
	Iterator  bench.ChangesetIterator
	Report    func()
}

func (opts TreeBuildOptions) With10_000() TreeBuildOptions {
	o := &opts
	o.Until = 10_000
	o.UntilHash = "460a9098015aef66f2da7f3d81fedf9a439ea3c3cf61723d535d2d94367858d5"
	return *o
}

func (opts TreeBuildOptions) With25_000() TreeBuildOptions {
	o := &opts
	o.Until = 25_000
	o.UntilHash = "a41235be12a8eedd007740ffc29fc55a5a169d693b1b3171982fe9c9034d55d6"
	return *o
}

func (opts TreeBuildOptions) With100_000() TreeBuildOptions {
	o := &opts
	o.Until = 100_000
	o.UntilHash = "e57ab75990453235859416baaccedbaac7b721cd099709ee968321c7822766b1"
	return *o
}

func (opts TreeBuildOptions) With300_000() TreeBuildOptions {
	o := &opts
	o.Until = 300_000
	o.UntilHash = "50a08008a29d76f3502d0a60c9e193a13efa6037a79a9f794652e1f97c2bbc16"
	return *o
}

func (opts TreeBuildOptions) With1_500_000() TreeBuildOptions {
	o := &opts
	o.Until = 1_500_000
	o.UntilHash = "ebc23d2e4e43075bae7ebc1e5db9d5e99acbafaa644b7c710213e109c8592099"
	return *o
}

func NewTreeBuildOptions() TreeBuildOptions {
	var seed int64 = 1234
	var versions int64 = 10_000_000
	bankGen := bench.BankLikeGenerator(seed, versions)
	//bankGen.InitialSize = 10_000
	lockupGen := bench.LockupLikeGenerator(seed, versions)
	//lockupGen.InitialSize = 10_000
	stakingGen := bench.StakingLikeGenerator(seed, versions)
	//stakingGen.InitialSize = 10_000
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

func BigTreeOptions_100_000() TreeBuildOptions {
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
	opts := TreeBuildOptions{
		Iterator:  itr,
		Until:     10_000,
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
		Until:     5_000,
		UntilHash: "c1dc9dc7d3a8ae025d2a347eea19121e98435b06b421607119bc3cf3cf79be05",
	}

	return opts
}
