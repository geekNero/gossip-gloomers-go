package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const (
	NodeCounterKey = "counter"
)

var (
	nodeCounterMu sync.RWMutex
	nodeCounter   int
	counterSum    int
	topology      map[string][]string
)

func put(kv *maelstrom.KV, n *maelstrom.Node) error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	err := kv.Write(ctx, fmt.Sprintf("%s-%s", NodeCounterKey, n.ID()), nodeCounter)
	if err != nil {
		return err
	}

	return nil
}

func main() {

	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)

	go func() {
		d := time.NewTicker(500 * time.Millisecond)
		for range d.C {
			nodeCounterMu.Lock()
			counterSum = nodeCounter
			for _, id := range n.NodeIDs() {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
				val, err := kv.ReadInt(ctx, fmt.Sprintf("%s-%s", NodeCounterKey, id))
				if err != nil {
					if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
						val = 0
					}
				}
				cancel()

				if id == n.ID() {
					continue
				}

				counterSum += val
			}

			nodeCounterMu.Unlock()
		}
	}()

	n.Handle("init", func(msg maelstrom.Message) error {
		// strip leading character 'n' from node ID
		// https://fly.io/dist-sys/1/ node id is "n1", "n2", ...

		nodeCounterMu.Lock()
		defer nodeCounterMu.Unlock()
		for _, id := range n.NodeIDs() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
			val, err := kv.ReadInt(ctx, fmt.Sprintf("%s-%s", NodeCounterKey, id))
			if err != nil {
				if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
					val = 0
				} else {
					return err
				}
			}

			if id == n.ID() {
				nodeCounter = val
			}

			counterSum += val
		}

		return nil
	})

	n.Handle("add", func(msg maelstrom.Message) error {

		var body struct {
			Delta int `json:"delta"`
		}

		err := json.Unmarshal(msg.Body, &body)
		if err != nil {
			return err
		}

		nodeCounterMu.Lock()
		nodeCounter += body.Delta
		counterSum += body.Delta
		err = put(kv, n)
		if err != nil {
			return err
		}
		nodeCounterMu.Unlock()

		return n.Reply(msg, map[string]any{"type": "add_ok"})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		nodeCounterMu.RLock()
		body := map[string]any{
			"type":  "read_ok",
			"value": counterSum,
		}
		nodeCounterMu.RUnlock()
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
