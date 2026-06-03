package main

import (
	"context"
	"encoding/json"
	"log"

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

	n.Handle("add", func(msg maelstrom.Message) error {
		var body struct {
			Delta int `json:"delta"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		for {
			// read latest value
			i, err := kv.ReadInt(context.Background(), "counter")
			if err != nil {
				if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
					i = 0
				} else {
					// TODO panic or what?
				}
			}
			err = kv.CompareAndSwap(context.Background(), "counter", i, i+body.Delta, true)
			if err != nil {
				if maelstrom.ErrorCode(err) == maelstrom.PreconditionFailed {
					// retry but if not precondition failed what to do?
				}
			} else {
				break
			}
		}

		return n.Reply(msg, map[string]any{
			"type": "add_ok",
		})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		// TODO I could make the counter an atomic and update it on read here? so the initial read
		// on add can be moved after the cas and only done on error
		i, err := kv.ReadInt(context.Background(), "counter")
		if err != nil {
			if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
				i = 0
				// TODO err or just return 0?
			}
		}

		return n.Reply(msg, map[string]any{
			"type":  "read_ok",
			"value": i,
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
