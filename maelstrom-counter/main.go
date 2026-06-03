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

// idea 1: store classic g-counter map of nodeID to increment under one key and then sum up on read
// TODO am I doing this correctly? with the sequential kv do I need to merge?

// idea 2: each node stores its int counter under its own key and on read reads every key from the
// kv as each node knows all node ids

// idea 3: global shared counter use cas & read
// TODO why does that not work? one of the nodes is simply always behind. if the test harness would
// wait longer it might catch up but it consistently fails on some node not having observed the
// latest state when reading from the kv
// https://jepsen.io/consistency/models/sequential
// > A process in a sequentially consistent system may be far ahead of, or behind, other processes.
// For instance, they may read arbitrarily stale state. However, once a process A has observed some
// operation from process B, it can never observe a state prior to B. This, combined with the total
// ordering property, makes sequential consistency a surprisingly strong model for programmers.

// idea 4: each node has its own counter but on read fans out to all other nodes to get their values
// and sums them up?
// feels silly that on every read I have to do N kv-reads
// also one node is again behind. which makes sense in a way I just increased the amount of kv
// interactions.

// idea 5: extend 4 but keep in memory counter on each node and on read reach out to other nodes and
// merge

// TODO on partition will I be able to talk to kv? I assume it might not be available

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)

	// TODO we would need to init the value on init to survive a crash
	// keep a node specific in memory counter
	var counter int
	var mu sync.RWMutex

	n.Handle("add", func(msg maelstrom.Message) error {
		var body struct {
			Delta int `json:"delta"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		mu.Lock()
		counter += body.Delta
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()
		err := kv.Write(ctx, n.ID(), counter)
		if err != nil {
			return err
		}
		mu.Unlock()

		return n.Reply(msg, map[string]any{
			"type": "add_ok",
		})
	})

	n.Handle("read_internal", func(msg maelstrom.Message) error {
		var i int
		mu.Lock()
		i = counter
		mu.Unlock()

		return n.Reply(msg, map[string]any{
			"type":  "read_internal_ok",
			"delta": i,
		})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		mu.RLock()
		sum := counter
		mu.RUnlock()

		for _, k := range n.NodeIDs() {
			if k == n.ID() {
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()
			msg, err := n.SyncRPC(ctx, k, map[string]any{
				"type": "read_internal",
			})
			if err != nil {
				return err
			}
			var body struct {
				Delta int `json:"delta"`
			}
			if err := json.Unmarshal(msg.Body, &body); err != nil {
				return err
			}
			sum += body.Delta
		}

		return n.Reply(msg, map[string]any{
			"type":  "read_ok",
			"value": sum,
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
