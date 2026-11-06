// Command vsrctl operates a VR cluster.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/derinil/vsr/kvservice"
	"github.com/derinil/vsr/vsr"
	"github.com/derinil/vsr/vsrclient"
	"github.com/derinil/vsr/vsrtcp"
)

const usage = `usage: vsrctl [-cluster a,b,c] [-timeout 30s] <command> [args]

commands:
  reconfigure CONFIG    replace the cluster configuration with CONFIG (comma separated)
  put KEY VALUE         store VALUE under KEY
  get KEY               print the value stored under KEY
  cas KEY FROM TO       replace FROM with TO under KEY if it currently holds FROM

KEY, VALUE, FROM and TO are parsed as JSON when valid, otherwise taken as strings.
`

func main() {
	if err := run(os.Args[1:], os.Getenv, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "vsrctl:", err)
		os.Exit(1)
	}
}

func run(args []string, getenv func(string) string, out io.Writer) error {
	fs := flag.NewFlagSet("vsrctl", flag.ExitOnError)
	cluster := fs.String("cluster", getenv("VSR_CLUSTER"), "comma separated replica addresses (env VSR_CLUSTER)")
	timeout := fs.Duration("timeout", 30*time.Second, "time limit for the command")
	fs.Usage = func() { fmt.Fprint(os.Stderr, usage) }
	fs.Parse(args)

	config := splitList(*cluster)
	switch {
	case len(config) == 0:
		return errors.New("-cluster (VSR_CLUSTER) is required")
	case fs.NArg() == 0:
		return errors.New("missing command")
	}

	cl, err := vsrclient.New(vsrclient.Options{
		Config: config,
		NewTransport: func(h vsr.Handler) vsr.Transport {
			return vsrtcp.New(h, vsrtcp.Options{})
		},
	})
	if err != nil {
		return err
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	cmd, rest := fs.Arg(0), fs.Args()[1:]
	switch cmd {
	case "reconfigure":
		if len(rest) != 1 {
			return errors.New("reconfigure takes one argument: CONFIG")
		}
		if err := cl.Reconfigure(ctx, splitList(rest[0])); err != nil {
			return err
		}
		fmt.Fprintf(out, "epoch %d: %s\n", cl.Epoch(), strings.Join(cl.Config(), ","))
	case "put", "get", "cas":
		op, err := kvOp(cmd, rest)
		if err != nil {
			return err
		}
		return kvCommand(ctx, cl, op, cmd, out)
	default:
		return fmt.Errorf("unknown command %q", cmd)
	}
	return nil
}

func kvOp(cmd string, args []string) ([]byte, error) {
	switch {
	case cmd == "put" && len(args) == 2:
		return kvservice.Write(parseArg(args[0]), parseArg(args[1]))
	case cmd == "get" && len(args) == 1:
		return kvservice.Read(parseArg(args[0]))
	case cmd == "cas" && len(args) == 3:
		return kvservice.CAS(parseArg(args[0]), parseArg(args[1]), parseArg(args[2]))
	}
	return nil, fmt.Errorf("wrong number of arguments for %s", cmd)
}

func kvCommand(ctx context.Context, cl *vsrclient.Client, op []byte, cmd string, out io.Writer) error {
	b, err := cl.Invoke(ctx, op)
	if err != nil {
		return err
	}
	r, err := kvservice.DecodeResult(b)
	if err != nil {
		return err
	}
	if r.Code != 0 {
		return fmt.Errorf("code %d: %s", r.Code, r.Error)
	}
	if cmd == "get" {
		fmt.Fprintln(out, string(r.Value))
	} else {
		fmt.Fprintln(out, "ok")
	}
	return nil
}

func parseArg(s string) any {
	if json.Valid([]byte(s)) {
		return json.RawMessage(s)
	}
	return s
}

func splitList(s string) []string {
	var out []string
	for item := range strings.SplitSeq(s, ",") {
		if item = strings.TrimSpace(item); item != "" {
			out = append(out, item)
		}
	}
	return out
}
