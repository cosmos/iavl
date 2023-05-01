# DBGenerator

`dbgenerator` is a command line tool to generate a `iavl` tree based on the legacy format of the node key.
This tool is used for testing the `lazy loading and set` feature of the `iavl` tree.

## Usage

It takes 4 arguments:
    - dbtype: the type of database to use. 
    - dbdir: the directory to store the database.
    - `random` or `sequential`: The `sequential` option will generate the tree from `1` to `version` in order. The `random` option will remove half of the versions randomly from the `sequential` mode.
    - version: the number of versions to generate.

```shell
go run main.go <dbtype> <dbdir> <random|sequential> <version>
```
