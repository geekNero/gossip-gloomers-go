package main

import (
	"fmt"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// https://fly.io/dist-sys/2/
// we receive
// {
//   "type": "generate"
// }
// we respond
// {
//   "type": "generate_ok",
//   "id": 123
// }
// snowflake ids: 64-bit 41-bit timestamp + 10-bit worker ID + 12-bit sequence number

func main() {
	n := maelstrom.NewNode()

	var counter uint32
	var mu sync.Mutex
	n.Handle("generate", func(msg maelstrom.Message) error {
		var id string
		// https://fly.io/dist-sys/1/ node id is "n1", "n2", ...
		id = n.ID()
		var current uint32
		mu.Lock()
		current = counter
		counter++
		mu.Unlock()
		body := map[string]any{
			"type": "generate_ok",
			"id":   fmt.Sprintf("%s%d", id, current),
		}

		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
