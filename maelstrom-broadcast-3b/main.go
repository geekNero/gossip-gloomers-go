package main

import (
	"encoding/json"
	"log"
	"maps"
	"math/rand"
	"slices"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var (
	mu       sync.RWMutex
	messages = map[int]struct{}{}
	topology map[string][]string
)

// https://fly.io/dist-sys/3b/
func main() {
	n := maelstrom.NewNode()
	// 2. gossip
	// pick uniformily random node of nodeIDs and broadcast to them
	n.Handle("broadcast", func(msg maelstrom.Message) error {
		// This message requests that a value be broadcast out to all nodes in the cluster.
		// The value is always an integer and it is unique for each message from Maelstrom.
		var body struct {
			Message int `json:"message"`
		}
		err := json.Unmarshal(msg.Body, &body)
		if err != nil {
			return err
		}

		mu.RLock()
		_, ok := messages[body.Message]
		mu.RUnlock()
		if !ok {
			mu.Lock()
			messages[body.Message] = struct{}{}
			mu.Unlock()
			// pick random node
			for i := 0; i < 3; {
				randomNode := n.NodeIDs()[rand.Intn(len(n.NodeIDs()))]
				if randomNode == n.ID() || randomNode == msg.Src {
					continue
				}
				n.Send(randomNode, map[string]any{
					"type":    "broadcast",
					"message": body.Message,
				})
				i++
			}
		}

		if msg.Src[0] == 'c' { // only reply to clients
			reply := map[string]any{
				"type": "broadcast_ok",
			}
			return n.Reply(msg, reply)
		}

		return nil
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		mu.RLock()
		body := map[string]any{
			"type":     "read_ok",
			"messages": slices.Collect(maps.Keys(messages)),
		}
		mu.RUnlock()
		return n.Reply(msg, body)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		var body struct {
			Topology map[string][]string `json:"topology"`
		}
		err := json.Unmarshal(msg.Body, &body)
		if err != nil {
			return err
		}

		topology = body.Topology

		reply := map[string]any{
			"type": "topology_ok",
		}
		return n.Reply(msg, reply)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
