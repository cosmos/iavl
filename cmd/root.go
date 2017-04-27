package cmd

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var RootCmd = &cobra.Command{
	Use:   "merkletool",
	Short: "Misc. database and benchmarking tools",
	Long: `Various tools that related to persistence.

Including:
	- Benchmark to check the underlying performance of the databases.
	- Dump to dump out the full contents of any persistent go-merkle database.
	`,
}

func Execute() {
	if err := RootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}
}

var (
	dbType string
	dbName string
)

func init() {
	cobra.OnInitialize(initEnv)
	RootCmd.PersistentFlags().StringVarP(&dbType, "dbType", "t", "goleveldb", "type of backing db")
	RootCmd.PersistentFlags().StringVarP(&dbName, "dbName", "d", "", "database cache name")
}

func initEnv() {
	viper.SetEnvPrefix("TM")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()
}
