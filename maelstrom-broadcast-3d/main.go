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
	trees    map[string]map[string][]string // spanning trees
)

// https://fly.io/dist-sys/3d/
//
// :stable-latencies {0 39,
//
//	0.5 459,
//	0.95 689,
//	0.99 775,
//	1 799},
func main() {
	n := maelstrom.NewNode()
	trees = make(map[string]map[string][]string)

	// TODO add anti-entropy loop to handle partition

	// TODO this now relies on topology being sent before first broadcast, can I rely on that?
	n.Handle("broadcast", func(msg maelstrom.Message) error {
		// This message requests that a value be broadcast out to all nodes in the cluster.
		// The value is always an integer and it is unique for each message from Maelstrom.
		var body struct {
			Message int    `json:"message"`
			Root    string `json:"root"`
		}
		err := json.Unmarshal(msg.Body, &body)
		if err != nil {
			return err
		}

		// if sender is a client this node is the root of the spanning tree otherwise the message is
		// a forwarded one with the root node set
		var root string
		if body.Root == "" {
			root = n.ID()
		} else {
			root = body.Root
		}

		mu.Lock()
		messages[body.Message] = struct{}{}
		tree, ok := trees[root]
		if !ok {
			tree = spanningTree(topology, root)
			trees[root] = spanningTree(topology, root)
		}
		mu.Unlock()

		// forward message using fire-and-forget to not incur cost of ack messages by each node.
		// anti-entropy loop takes care of lost messages/partitions
		nodes := tree[n.ID()]
		for _, node := range nodes {
			go func() {
				_ = n.Send(node, map[string]any{
					"type":    "broadcast",
					"message": body.Message,
					"root":    root,
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
