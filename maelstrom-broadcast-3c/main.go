package main

import (
	"encoding/json"
	"log"
	"maps"
	"math/rand"
	"slices"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var (
	mu       sync.RWMutex
	messages = map[int]struct{}{}
)

// Challenge #3c: Fault Tolerant Broadcast
// https://fly.io/dist-sys/3c/
func main() {
	n := maelstrom.NewNode()

	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		for range ticker.C {
			mu.RLock()
			body := map[string]any{
				"type":     "sync",
				"messages": slices.Collect(maps.Keys(messages)),
			}
			mu.RUnlock()

			// gossip: messages to 3 random peers (assumes --node-count 5)
			for i := 0; i < 3; {
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
			Message []int `json:"messages"`
		}
		err := json.Unmarshal(msg.Body, &body)
		if err != nil {
			return err
		}

		mu.Lock()
		for _, v := range body.Message {
			messages[v] = struct{}{}
		}
		mu.Unlock()

		return nil
	})

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

		mu.Lock()
		_, ok := messages[body.Message]
		if !ok {
			messages[body.Message] = struct{}{}
		}
		mu.Unlock()
		if !ok {
			// gossip: forward new messages to 3 random peers (assumes --node-count 5)
			for i := 0; i < 3; {
				randomNode := n.NodeIDs()[rand.Intn(len(n.NodeIDs()))]
				if randomNode == n.ID() || randomNode == msg.Src {
					continue
				}
				_ = n.Send(randomNode, map[string]any{
					"type":    "broadcast",
					"message": body.Message,
				})
				i++
			}
		}

		if msg.Src[0] == 'c' { // only reply to clients
			return n.Reply(msg, map[string]any{
				"type": "broadcast_ok",
			})
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
		return n.Reply(msg, map[string]any{
			"type": "topology_ok",
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
