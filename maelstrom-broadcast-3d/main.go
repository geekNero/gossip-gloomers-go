package main

import (
	"encoding/json"
	"log"
	"maps"
	"math/rand"
	_ "net/http/pprof"
	"slices"
	"sync"
	"time"

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

	// n.Handle("init", func(msg maelstrom.Message) error {
	// 	// n<number> -> base port 6060 + number, e.g. n3 -> 6063
	// 	num, err := strconv.Atoi(n.ID()[1:])
	// 	if err != nil {
	// 		return fmt.Errorf("parse node id %q: %w", n.ID(), err)
	// 	}
	// 	go func() { log.Fatal(http.ListenAndServe(fmt.Sprintf("localhost:%d", 6060+num), nil)) }()
	// 	return nil
	// })

	// 500ms interval
	// :msg-count 71598, (1s interval)
	//                  :msgs-per-op 33.773567},
	//             :stable-latencies {0 0,
	// 0.5 386,
	// 0.95 738,
	// 0.99 948,
	// 1 1236},
	// 967 values
	// N=25
	// 25 nodes * 3 message/s * 20s = 1500 messages
	go func() {
		d := time.NewTicker(50 * time.Millisecond)
		for range d.C {
			var body map[string]any
			mu.RLock()
			body = map[string]any{
				"type":     "timed_broadcast",
				"messages": slices.Collect(maps.Keys(messages)),
			}
			mu.RUnlock()

			for i := 0; i < 3; {
				randomNode := n.NodeIDs()[rand.Intn(len(n.NodeIDs()))]
				if randomNode == n.ID() {
					continue
				}
				go n.Send(randomNode, body)
				i++
			}
		}
	}()

	n.Handle("timed_broadcast", func(msg maelstrom.Message) error {
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

		// mu.RLock()
		// _, ok := messages[body.Message]
		// mu.RUnlock()
		// if !ok {
		mu.Lock()
		messages[body.Message] = struct{}{}
		mu.Unlock()
		// 	// pick random node
		// 	for i := 0; i < 6; {
		// 		randomNode := n.NodeIDs()[rand.Intn(len(n.NodeIDs()))]
		// 		if randomNode == n.ID() || randomNode == msg.Src {
		// 			continue
		// 		}
		// 		go n.Send(randomNode, map[string]any{
		// 			"type":    "broadcast",
		// 			"message": body.Message,
		// 		})
		// 		i++
		// 	}
		// }

		// if msg.Src[0] == 'c' { // only reply to clients
		reply := map[string]any{
			"type": "broadcast_ok",
		}
		return n.Reply(msg, reply)
		// }

		// return nil
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
