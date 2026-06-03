package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// Challenge #4: Grow-Only Counter
// https://fly.io/dist-sys/4/
// Implements a state-based G-Counter CRDT per Shapiro et al., "A comprehensive study of
// Convergent and Commutative Replicated Data Types" https://inria.hal.science/inria-00555588/document

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)

	var mu sync.RWMutex
	counters := make(map[string]int)

	// init the nodes own delta to survive a crash
	n.Handle("init", func(msg maelstrom.Message) error {
		mu.Lock()
		defer mu.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()
		delta, err := kv.ReadInt(ctx, n.ID())
		if err != nil {
			if maelstrom.ErrorCode(err) != maelstrom.KeyDoesNotExist {
				return err
			}
			delta = 0
		}
		counters[n.ID()] += delta

		return nil
	})

	// seq-kv may serve past states indefinitely to other nodes: a node is allowed to observe any
	// prefix of the total order, so another node's writes may never become visible via kv reads.
	// Two approaches to get a consistent counter value across nodes:
	// 1. On read, RPC to each peer to fetch their local counter directly (not via seq-kv).
	// 2. Periodically broadcast each node's local counter to all peers (this approach) so reads
	//    stay local and the G-Counter merge (max) converges.
	// With only 3 nodes, broadcasting to all peers is cheap; no need for random peer selection.
	go func() {
		ticker := time.NewTicker(time.Second)
		for range ticker.C {
			mu.RLock()
			body := map[string]any{
				"type":     "sync",
				"counters": counters,
			}
			mu.RUnlock()

			for _, peer := range n.NodeIDs() {
				if peer != n.ID() {
					_ = n.Send(peer, body)
				}
			}
		}
	}()

	n.Handle("sync", func(msg maelstrom.Message) error {
		var body struct {
			Counters map[string]int `json:"counters"`
		}
		err := json.Unmarshal(msg.Body, &body)
		if err != nil {
			return err
		}

		mu.Lock()
		for node, delta := range body.Counters {
			counters[node] = max(counters[node], delta)
		}
		mu.Unlock()

		return nil
	})

	n.Handle("add", func(msg maelstrom.Message) error {
		var body struct {
			Delta int `json:"delta"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		mu.Lock()
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()
		err := kv.Write(ctx, n.ID(), counters[n.ID()])
		if err != nil {
			mu.Unlock()
			return err
		}
		counters[n.ID()] += body.Delta // only inc local g-counter if write to kv-store succeeds
		mu.Unlock()

		return n.Reply(msg, map[string]any{
			"type": "add_ok",
		})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		mu.RLock()
		var value int
		for _, delta := range counters {
			value += delta
		}
		mu.RUnlock()

		return n.Reply(msg, map[string]any{
			"type":  "read_ok",
			"value": value,
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
