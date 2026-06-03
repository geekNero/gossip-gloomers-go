package main

import (
	"context"
	"encoding/json"
	"log"
	"maps"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// Challenge #4: Grow-Only Counter
// https://fly.io/dist-sys/4/

// idea 1: store classic g-counter map of nodeID to increment under one key and then sum up on read
// TODO am I doing this correctly? with the sequential kv do I need to merge?
// idea 2: each node stores its int counter under its own key and on read reads every key from the
// kv as each node knows all node ids

// TODO on partition will I be able to talk to kv?

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)

	var counter map[string]int
	var mu sync.RWMutex

	n.Handle("add", func(msg maelstrom.Message) error {
		var body struct {
			Delta int `json:"delta"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		newCounter := make(map[string]int, len(n.NodeIDs()))
		mu.RLock()
		maps.Copy(newCounter, counter)
		newCounter[n.ID()] += body.Delta
		err := kv.CompareAndSwap(context.Background(), "counter", counter, newCounter, true)
		if err != nil {
			// TODO what now; retry I guess
			if maelstrom.ErrorCode(err) == maelstrom.PreconditionFailed {
				// retry
			}
		}
		mu.RUnlock()

		return n.Reply(msg, map[string]any{
			"type": "add_ok",
		})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		result := make(map[string]int)
		err := kv.ReadInto(context.Background(), "counter", &result)
		if err != nil {
			// TODO err or just return 0?
		}

		var sum int
		for _, v := range result {
			sum += v
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
