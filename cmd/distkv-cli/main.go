// distkv-cli is a command-line client for DistKV.
//
//	distkv-cli -cluster localhost:7001,localhost:7002,localhost:7003 put mykey myvalue
//	distkv-cli -cluster ... get mykey
//	distkv-cli -cluster ... delete mykey
//	distkv-cli -cluster ... status
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/rohithreddy/distkv/client"
)

func main() {
	cluster := flag.String("cluster", "localhost:7001,localhost:7002,localhost:7003", "comma-separated KV addresses")
	timeout := flag.Duration("timeout", 10*time.Second, "operation timeout")
	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "usage: distkv-cli [flags] get|put|delete|status [args]")
		os.Exit(2)
	}
	addrs := strings.Split(*cluster, ",")
	c := client.New(addrs)
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	switch args[0] {
	case "get":
		requireArgs(args, 2)
		val, found, err := c.Get(ctx, args[1])
		die(err)
		if !found {
			fmt.Println("(not found)")
			os.Exit(1)
		}
		fmt.Println(string(val))
	case "put":
		requireArgs(args, 3)
		die(c.Put(ctx, args[1], []byte(args[2])))
		fmt.Println("OK")
	case "delete":
		requireArgs(args, 2)
		die(c.Delete(ctx, args[1]))
		fmt.Println("OK")
	case "status":
		for _, addr := range addrs {
			st, err := c.Status(ctx, addr)
			if err != nil {
				fmt.Printf("%-22s unreachable: %v\n", addr, err)
				continue
			}
			role := "follower"
			if st.IsLeader {
				role = "LEADER"
			}
			fmt.Printf("%-22s id=%d %-8s term=%d commit=%d applied=%d\n",
				addr, st.NodeId, role, st.Term, st.CommitIndex, st.AppliedIndex)
		}
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n", args[0])
		os.Exit(2)
	}
}

func requireArgs(args []string, n int) {
	if len(args) < n {
		fmt.Fprintf(os.Stderr, "%s: missing arguments\n", args[0])
		os.Exit(2)
	}
}

func die(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
