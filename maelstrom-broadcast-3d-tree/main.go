package main

import (
	"encoding/json"
	"fmt"
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

// Challenge #3d: Efficient Broadcast, Part I
// https://fly.io/dist-sys/3d/
//
// We will increase our node count to 25 and add a delay of 100ms to each message to simulate a slow network.
// Your challenge is to achieve the following:
// Messages-per-operation is below 30
// Median latency is below 400ms
// Maximum latency is below 600ms
//
// Numbers considered stale because broadcast_ok is returned without confirming the message has
// been acknowledged by other nodes, so a client can race ahead to read from another node before
// the message propagates there. See:
// https://github.com/jepsen-io/maelstrom/blob/main/doc/03-broadcast/02-performance.md

// branch is the branching factor of the spanning tree. With 25 nodes, branch=5 gives a depth-2
// tree (1 root + 5 children + 20 grandchildren = 26 slots, 25 used), minimising latency to 2
// network hops while keeping msgs-per-op at the theoretical minimum of N-1=24. See results/ for
// a comparison of branching factors.
const branch = 5

func main() {
	n := maelstrom.NewNode()

	go func() {
		ticker := time.NewTicker(1 * time.Second)
		for range ticker.C {
			mu.RLock()
			body := map[string]any{
				"type":     "sync",
				"messages": slices.Collect(maps.Keys(messages)),
			}
			mu.RUnlock()

			// gossip: messages to 3 random peers (assumes --node-count 25)
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

		// forward message using fire-and-forget to not incur cost of ack messages by each node.
		nodes := children(n.NodeIDs(), root, n.ID(), branch)
		for _, node := range nodes {
			go func() {
				_ = n.Send(node, map[string]any{
					"type":    "broadcast",
					"message": body.Message,
					"root":    root,
				})
			}()
		}

		mu.Lock()
		messages[body.Message] = struct{}{}
		mu.Unlock()

		if msg.Src[0] == 'c' { // only reply to clients as forwarding is fire-and-forget
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

// children returns the children of parent in a spanning tree rooted at root with the given
// branching factor, using the globally consistent node ordering from NodeIDs. NodeIDs returns the
// same ordered list on every node, which is the invariant that makes the implicit tree structure
// work without coordination.
func children(nodes []string, root, parent string, branch int) []string {
	rootIdx := slices.Index(nodes, root)
	if rootIdx == -1 {
		panic(fmt.Errorf("root %q not found in list of nodes %s", root, nodes))
	}
	parentIdx := slices.Index(nodes, parent)
	if parentIdx == -1 {
		panic(fmt.Errorf("parent %q not found in list of nodes %s", parent, nodes))
	}
	// children are at [branch*i+1,...,branch*i+1+branch-1]
	logicalParentIdx := ((parentIdx - rootIdx) + len(nodes)) % len(nodes)
	childIdx := logicalParentIdx*branch + 1
	if childIdx > len(nodes)-1 { // a childs index must exist i.e. branch*i+1<=len(nodes)-1
		return nil
	}

	var result []string
	for i := range branch {
		if childIdx+i >= len(nodes) {
			break
		}
		result = append(result, nodes[(childIdx+i+rootIdx)%len(nodes)])
	}
	return result
}
