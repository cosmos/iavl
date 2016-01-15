package main

import (
	"github.com/codegangsta/cli"
	. "github.com/tendermint/go-common"
	"os"

	"github.com/tendermint/merkleeyes/server"
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
					Value: "tcp://127.0.0.1:46659",
					Usage: "MerkleEyes server listen address",
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
	_, err := server.StartListener(addr)
	if err != nil {
		Exit(err.Error())
	}

	// Sleep forever and then...
	TrapSignal(func() {
	})
}
