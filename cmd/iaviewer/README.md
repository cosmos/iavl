# IaViewer

`iaviewer` is a utility to inspect the contents of a persisted iavl tree, given (a copy of) the leveldb store.
This can be quite useful for debugging, especially when you find odd errors, or non-deterministic behavior.
Below is a brief introduction to the tool.

## Installation

Once this is merged into the offical repo, master, you should be able to do:

```shell
go get github.com/tendermint/iavl
cd $(GOPATH)/src/github.com/tendermint/iavl
make get_vendor_deps
make install
```

However, as this PR lives in another repo on another branch, it is a bit trickier to build.
If you are not familiar with the nuances of compiling forks of go projects, follow the
steps below:

```shell
# note we must check into the original path
go get github.com/tendermint/iavl
cd $(GOPATH)/src/github.com/tendermint/iavl

# this is needed to check out the proper version
git remote add pr https://github.com/iov-one/iavl.git
git fetch pr
git checkout iaviewer

make get_vendor_deps
make install
```