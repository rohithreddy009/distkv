// distkv-cli is a command-line client for DistKV.
//
//	distkv-cli -cluster localhost:7001,localhost:7002,localhost:7003 put mykey myvalue
//	distkv-cli -cluster ... members list
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/rohithreddy/distkv/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/rohithreddy/distkv/proto/kvpb"
)

func main() {
	cluster := flag.String("cluster", "localhost:7001,localhost:7002,localhost:7003", "comma-separated KV addresses")
	timeout := flag.Duration("timeout", 10*time.Second, "operation timeout")
	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "usage: distkv-cli [flags] get|put|delete|status|members [args]")
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
	case "members":
		die(membersCmd(ctx, addrs, args[1:]))
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n", args[0])
		os.Exit(2)
	}
}

func membersCmd(ctx context.Context, addrs []string, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("members: need list|add|remove")
	}
	kv, err := dialKV(addrs[0])
	if err != nil {
		return err
	}
	switch args[0] {
	case "list":
		resp, err := kv.ListMembers(ctx, &kvpb.ListMembersRequest{})
		if err != nil {
			return err
		}
		if resp.InJoint {
			fmt.Println("(joint config transition in progress)")
		}
		for _, m := range resp.Members {
			fmt.Printf("id=%d raft=%s voting=%v\n", m.Id, m.RaftAddr, m.Voting)
		}
		return nil
	case "add":
		requireArgs(append([]string{"add"}, args...), 2)
		id, addr, err := parseMemberSpec(args[1])
		if err != nil {
			return err
		}
		_, err = kv.AddMember(ctx, &kvpb.AddMemberRequest{Id: id, RaftAddr: addr})
		return err
	case "remove":
		requireArgs(append([]string{"remove"}, args...), 2)
		id, err := strconv.ParseUint(args[1], 10, 64)
		if err != nil {
			return fmt.Errorf("bad member id: %w", err)
		}
		_, err = kv.RemoveMember(ctx, &kvpb.RemoveMemberRequest{Id: id})
		return err
	default:
		return fmt.Errorf("unknown members subcommand %q", args[0])
	}
}

func parseMemberSpec(spec string) (uint64, string, error) {
	parts := strings.SplitN(spec, "=", 2)
	if len(parts) != 2 {
		return 0, "", fmt.Errorf("expected id=host:port, got %q", spec)
	}
	id, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return 0, "", err
	}
	return id, parts[1], nil
}

func dialKV(addr string) (kvpb.KVClient, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return kvpb.NewKVClient(conn), nil
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
