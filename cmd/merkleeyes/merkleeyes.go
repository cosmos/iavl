package main

import (
	"github.com/tendermint/abci/server"
	. "github.com/tendermint/go-common"
	"github.com/urfave/cli"
	"os"

	application "github.com/tendermint/merkleeyes/app"
)

func main() {
	app := cli.NewApp()
	app.Name = "cli"
	app.Usage = "cli [command] [args...]"
	app.Commands = []cli.Command{
		{
			Name:  "server",
			Usage: "Run the MerkleEyes server",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "address",
					Value: "unix://data.sock",
					Usage: "MerkleEyes server listen address",
				},
				cli.StringFlag{
					Name:  "abci",
					Value: "socket",
					Usage: "socket | grpc",
				},
				cli.StringFlag{
					Name:  "dbName",
					Value: "", //empty strings create non-persistent db
					Usage: "database name",
				},
				cli.IntFlag{
					Name:  "cache",
					Value: 0,
					Usage: "database cache size",
				},
			},
			Action: func(c *cli.Context) {
				cmdServer(app, c)
			},
		},
	}
	app.Run(os.Args)

}

//--------------------------------------------------------------------------------

func cmdServer(app *cli.App, c *cli.Context) {
	addr := c.String("address")
	abci := c.String("abci")
	dbName := c.String("dbName")
	cache := c.Int("cache")

	// Start the listener
	mApp := application.NewMerkleEyesApp(dbName, cache)
	s, err := server.NewServer(addr, abci, mApp)

	if err != nil {
		Exit(err.Error())
	}

	// Wait forever
	TrapSignal(func() {
		// Cleanup
		mApp.CloseDB()
		s.Stop()
	})
}
