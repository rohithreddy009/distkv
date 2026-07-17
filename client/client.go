// Package client provides a DistKV client with automatic leader discovery
// and retry on leader changes.
package client

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/rohithreddy/distkv/proto/kvpb"
)

type Client struct {
	mu      sync.Mutex
	addrs   []string
	conns   map[string]kvpb.KVClient
	leader  string // last known leader address ("" = unknown)
	id      string
	seq     atomic.Uint64
	timeout time.Duration
}

func New(addrs []string) *Client {
	return &Client{
		addrs:   addrs,
		conns:   map[string]kvpb.KVClient{},
		id:      fmt.Sprintf("c%d-%d", time.Now().UnixNano(), rand.Int63()),
		timeout: 5 * time.Second,
	}
}

func (c *Client) conn(addr string) (kvpb.KVClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if cl, ok := c.conns[addr]; ok {
		return cl, nil
	}
	gc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	cl := kvpb.NewKVClient(gc)
	c.conns[addr] = cl
	return cl, nil
}

// try runs fn against the last known leader first, then every node,
// retrying with backoff until ctx expires.
func (c *Client) try(ctx context.Context, fn func(kvpb.KVClient) error) error {
	backoff := 20 * time.Millisecond
	var lastErr error
	for {
		c.mu.Lock()
		order := make([]string, 0, len(c.addrs)+1)
		if c.leader != "" {
			order = append(order, c.leader)
		}
		for _, a := range c.addrs {
			if a != c.leader {
				order = append(order, a)
			}
		}
		c.mu.Unlock()

		for _, addr := range order {
			if ctx.Err() != nil {
				return fmt.Errorf("distkv: %w (last: %v)", ctx.Err(), lastErr)
			}
			cl, err := c.conn(addr)
			if err != nil {
				lastErr = err
				continue
			}
			if err := fn(cl); err != nil {
				lastErr = err
				c.mu.Lock()
				if c.leader == addr {
					c.leader = ""
				}
				c.mu.Unlock()
				continue
			}
			c.mu.Lock()
			c.leader = addr
			c.mu.Unlock()
			return nil
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("distkv: %w (last: %v)", ctx.Err(), lastErr)
		case <-time.After(backoff):
		}
		if backoff < 500*time.Millisecond {
			backoff *= 2
		}
	}
}

func (c *Client) Put(ctx context.Context, key string, value []byte) error {
	req := &kvpb.PutRequest{Key: key, Value: value, ClientId: c.id, Seq: c.seq.Add(1)}
	return c.try(ctx, func(cl kvpb.KVClient) error {
		cctx, cancel := context.WithTimeout(ctx, c.timeout)
		defer cancel()
		_, err := cl.Put(cctx, req)
		return err
	})
}

func (c *Client) Delete(ctx context.Context, key string) error {
	req := &kvpb.DeleteRequest{Key: key, ClientId: c.id, Seq: c.seq.Add(1)}
	return c.try(ctx, func(cl kvpb.KVClient) error {
		cctx, cancel := context.WithTimeout(ctx, c.timeout)
		defer cancel()
		_, err := cl.Delete(cctx, req)
		return err
	})
}

// Get returns (value, found).
func (c *Client) Get(ctx context.Context, key string) ([]byte, bool, error) {
	var val []byte
	var found bool
	err := c.try(ctx, func(cl kvpb.KVClient) error {
		cctx, cancel := context.WithTimeout(ctx, c.timeout)
		defer cancel()
		resp, err := cl.Get(cctx, &kvpb.GetRequest{Key: key})
		if err != nil {
			return err
		}
		val, found = resp.Value, resp.Found
		return nil
	})
	return val, found, err
}

// Status queries one node directly (no leader routing).
func (c *Client) Status(ctx context.Context, addr string) (*kvpb.StatusResponse, error) {
	cl, err := c.conn(addr)
	if err != nil {
		return nil, err
	}
	cctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return cl.Status(cctx, &kvpb.StatusRequest{})
}

// Addrs returns the configured cluster addresses.
func (c *Client) Addrs() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.addrs...)
}

func (c *Client) ListMembers(ctx context.Context) (*kvpb.ListMembersResponse, error) {
	var out *kvpb.ListMembersResponse
	err := c.try(ctx, func(cl kvpb.KVClient) error {
		cctx, cancel := context.WithTimeout(ctx, c.timeout)
		defer cancel()
		resp, err := cl.ListMembers(cctx, &kvpb.ListMembersRequest{})
		if err != nil {
			return err
		}
		out = resp
		return nil
	})
	return out, err
}

func (c *Client) AddMember(ctx context.Context, id uint64, raftAddr string) error {
	return c.try(ctx, func(cl kvpb.KVClient) error {
		cctx, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		_, err := cl.AddMember(cctx, &kvpb.AddMemberRequest{Id: id, RaftAddr: raftAddr})
		return err
	})
}

func (c *Client) RemoveMember(ctx context.Context, id uint64) error {
	return c.try(ctx, func(cl kvpb.KVClient) error {
		cctx, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		_, err := cl.RemoveMember(cctx, &kvpb.RemoveMemberRequest{Id: id})
		return err
	})
}
