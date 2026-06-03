package main

import (
	"context"
	"encoding/json"
	"log"
	"math/rand"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// Challenge #4: Grow-Only Counter
// https://fly.io/dist-sys/4/

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

	// gossip loop to observe fresh values as sequential kv does not guarantee freshness of values
	// written by other nodes
	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		for range ticker.C {
			mu.RLock()
			body := map[string]any{
				"type":     "sync",
				"counters": counters,
			}
			mu.RUnlock()

			for i := 0; i < 1; {
				randomNode := n.NodeIDs()[rand.Intn(len(n.NodeIDs()))]
				if randomNode == n.ID() {
					continue
				}
				_ = n.Send(randomNode, body)
				i++
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
