package main

import (
	"encoding/json"
	"log"
	"maps"
	_ "net/http/pprof"
	"slices"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var (
	mu       sync.RWMutex
	messages = map[int]struct{}{}
	topology map[string][]string
)

// https://fly.io/dist-sys/3d/
func main() {
	n := maelstrom.NewNode()

	// TODO add anti-entropy loop to handle partition

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

		// if sender is a client this node is the root of the spanning tree
		var root string
		if msg.Src[0] == 'c' {
			root = n.ID()
		} else {
			// TODO we must set the original sender to get the original root and thus its spanning
			// tree
			root = msg.Src
		}

		mu.Lock()
		messages[body.Message] = struct{}{}
		tree := spanningTree(topology, root)
		mu.Unlock()

		// forward message using fire-and-forget to not incur cost of ack messages by each node.
		// anti-entropy loop takes care of lost messages/partitions
		nodes := tree[n.ID()]
		for _, node := range nodes {
			go func() {
				_ = n.Send(node, map[string]any{
					"type":    "broadcast",
					"message": body.Message,
				})
			}()
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

		mu.Lock()
		topology = body.Topology
		mu.Unlock()

		reply := map[string]any{
			"type": "topology_ok",
		}
		return n.Reply(msg, reply)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func spanningTree(graph map[string][]string, root string) map[string][]string {
	result := make(map[string][]string)
	visited := make(map[string]struct{})
	queue := []string{root}
	for len(queue) > 0 {
		n := queue[0]
		queue = queue[1:]
		visited[n] = struct{}{}
		for _, m := range graph[n] {
			if _, ok := visited[m]; ok {
				continue
			}
			visited[m] = struct{}{}
			result[n] = append(result[n], m)
			queue = append(queue, m)
		}
	}
	return result
}
