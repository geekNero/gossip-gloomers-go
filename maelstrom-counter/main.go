package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// Challenge #4: Grow-Only Counter
// https://fly.io/dist-sys/4/

var (
	mu      sync.Mutex
	counter int
)

func main() {
	n := maelstrom.NewNode()

	n.Handle("add", func(msg maelstrom.Message) error {
		var body struct {
			Delta int `json:"delta"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// TODO: implement

		return n.Reply(msg, map[string]any{
			"type": "add_ok",
		})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		// TODO: implement

		return n.Reply(msg, map[string]any{
			"type":  "read_ok",
			"value": 0,
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
