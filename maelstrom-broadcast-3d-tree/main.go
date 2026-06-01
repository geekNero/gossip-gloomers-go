package main

import (
	"encoding/json"
	"fmt"
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
)

// Challenge #3d: Efficient Broadcast, Part I
// https://fly.io/dist-sys/3d/
//
// TODO cleanup current implementation
// TODO add anti-entropy loop to handle partition
// TODO make visualizations and godocs for the binary tree
// TODO commit results.edn for each of the approaches
// TODO why are the numbers considered stale?
// https://github.com/jepsen-io/maelstrom/blob/main/doc/03-broadcast/02-performance.md
// > That's sort of expected: we return a broadcast_ok without trying to confirm that the message has been acknowledged by anyone else, so of course another client could race ahead to observe another node before the message has propagated there.

func main() {
	n := maelstrom.NewNode()

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
		// anti-entropy loop takes care of lost messages/partitions
		nodes := children(n.NodeIDs(), root, n.ID())
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

// TODO godoc that this depends on this invariant
// NodeIDs returns a list of all node IDs in the cluster. This list include the
// local node ID and is the same order across all nodes. Only valid after "init"
// message has been received.

// branch 2
//
//	:by-f {:broadcast {:valid? true,
//	                  :count 1004,
//	                  :ok-count 1004,
//	                  :fail-count 0,
//	                  :info-count 0},
//	      :read {:valid? true,
//	             :count 974,
//	             :ok-count 974,
//	             :fail-count 0,
//	             :info-count 0}}},
//
// :availability {:valid? true, :ok-fraction 1.0},
// :net {:all {:send-count 28152,
//
//	            :recv-count 28152,
//	            :msg-count 28152,
//	            :msgs-per-op 14.232558},
//	      :clients {:send-count 4056, :recv-count 4056, :msg-count 4056},
//	      :servers {:send-count 24096,
//	                :recv-count 24096,
//	                :msg-count 24096,
//	                :msgs-per-op 12.182002}
//		:stable-latencies {0 0,
//
// 0.5 290,
// 0.95 396,
// 0.99 398,
// 1 399}

// branch 3
//
//	:by-f {:broadcast {:valid? true,
//	                  :count 965,
//	                  :ok-count 965,
//	                  :fail-count 0,
//	                  :info-count 0},
//	      :read {:valid? true,
//	             :count 1033,
//	             :ok-count 1033,
//	             :fail-count 0,
//	             :info-count 0}}},
//
// :availability {:valid? true, :ok-fraction 1.0},
// :net {:all {:send-count 27256,
//
//	            :recv-count 27256,
//	            :msg-count 27256,
//	            :msgs-per-op 13.641642},
//	      :clients {:send-count 4096, :recv-count 4096, :msg-count 4096},
//	      :servers {:send-count 23160,
//	                :recv-count 23160,
//	                :msg-count 23160,
//	                :msgs-per-op 11.591592},
//		:stable-latencies {0 0,
//
// 0.5 203,
// 0.95 291,
// 0.99 298,
// 1 303}

// branch 4
//
//	:servers {:send-count 24864,
//
// :recv-count 24864,
// :msg-count 24864,
// :msgs-per-op 12.444445}
//
//	:stable-latencies {0 0,
//
// 0.5 200,
// 0.95 269,
// 0.99 289,
// 1 296}

// branch 5
//
//	:by-f {:broadcast {:valid? true,
//	                  :count 992,
//	                  :ok-count 992,
//	                  :fail-count 0,
//	                  :info-count 0},
//	      :read {:valid? true,
//	             :count 1003,
//	             :ok-count 1003,
//	             :fail-count 0,
//	             :info-count 0}}},
//
// :availability {:valid? true, :ok-fraction 1.0},
// :net {:all {:send-count 27898,
//
//	            :recv-count 27898,
//	            :msg-count 27898,
//	            :msgs-per-op 13.98396},
//	      :clients {:send-count 4090, :recv-count 4090, :msg-count 4090},
//	      :servers {:send-count 23808,
//	                :recv-count 23808,
//	                :msg-count 23808,
//	                :msgs-per-op 11.933835}
//		:stable-latencies {0 0,
//
// 0.5 172,
// 0.95 196,
// 0.99 201,
// 1 211}

// branch 6
//         :by-f {:broadcast {:valid? true,
//                            :count 980,
//                            :ok-count 980,
//                            :fail-count 0,
//                            :info-count 0},
//                :read {:valid? true,
//                       :count 1019,
//                       :ok-count 1019,
//                       :fail-count 0,
//                       :info-count 0}}},
// :availability {:valid? true, :ok-fraction 1.0},
// :net {:all {:send-count 27618,
//             :recv-count 27618,
//             :msg-count 27618,
//             :msgs-per-op 13.8159075},
//       :clients {:send-count 4098, :recv-count 4098, :msg-count 4098},
//       :servers {:send-count 23520,
//                 :recv-count 23520,
//                 :msg-count 23520,
//                 :msgs-per-op 11.7658825}
// :stable-latencies {0 0,
// 0.5 175,
// 0.95 198,
// 0.99 201,
// 1 212}

func children(nodes []string, root, parent string) []string {
	rootIdx := slices.Index(nodes, root)
	if rootIdx == -1 {
		panic(fmt.Errorf("root %q not found in list of nodes %s", root, nodes))
	}
	parentIdx := slices.Index(nodes, parent)
	if parentIdx == -1 {
		panic(fmt.Errorf("parent %q not found in list of nodes %s", parent, nodes))
	}
	branch := 5
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
