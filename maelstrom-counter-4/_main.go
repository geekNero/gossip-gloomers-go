package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"slices"
	"strconv"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const (
	CounterKey       = "counter"
	SequenceKey      = "sequence"
	LastMessageIDKey = "last_message_id"
	DeltaBufferKey   = "delta_key"
)

type Msg struct {
	RefCounter int `json:"ref"`
	Value      int `json:"value"`
}

func (msg *Msg) Less(int)

var (
	globalCounterMu sync.RWMutex
	globalCounter   int
	sequenceCounter int
	topology        map[string][]string
	lastMessageID   []int
	inBuffer        = map[string][]Msg{}
	deltaBuffer     []int
	inBufferMu      sync.RWMutex
)

func getNodeID(id string) int {
	nodeID, _ := strconv.Atoi(id[1:])
	return nodeID
}

func put(kv *maelstrom.KV) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	globalCounterMu.RLock()
	defer globalCounterMu.RUnlock()
	err := kv.Write(ctx, CounterKey, globalCounter)
	if err != nil {
		return err
	}
	err = kv.Write(ctx, SequenceKey, sequenceCounter)
	if err != nil {
		return err
	}
	err = kv.Write(ctx, DeltaBufferKey, deltaBuffer)
	if err != nil {
		return err
	}

	return nil
}

func forward(msgs []Msg, root string, n *maelstrom.Node) error {

	body := map[string]any{
		"root":     n.ID(),
		"messages": msgs,
		"type":     "gossip",
	}

	for range 3 {
		randomNode := n.NodeIDs()[rand.Intn(len(n.NodeIDs()))]
		if randomNode == n.ID() || randomNode == root {
			continue
		}
		n.Send(randomNode, body)
	}

	return nil
}

// goal with fetchMessage is to let node B know how many of it's messages were received
// by node A, and receive the difference from node B. If node B is partitioned, then ask
// some other node to forward the request.
// func fetchMessage(targetNode string, ref int, n *maelstrom.Node) {

// 	body := map[string]any{
// 		"type":   "fetch",
// 		"target": targetNode,
// 		"ref":    ref,
// 	}

// 	var msgUpdated bool

// 	handlerFunc := func(msg maelstrom.Message) error {

// 		var body struct {
// 			Delta int `json:"delta"`
// 			Ref   int `json:"ref"`
// 		}

// 		err := json.Unmarshal(msg.Body, &body)
// 		if err != nil {
// 			return err
// 		}

// 		if body.Ref > ref {
// 			globalCounterMu.Lock()
// 			globalCounter += body.Delta
// 			lastMessageID[body.Ref] = ref
// 			globalCounterMu.Unlock()

// 			msgUpdated = true
// 		}

// 		return nil
// 	}
// 	index := 0
// 	target := targetNode

// 	for msgUpdated {
// 		n.RPC(target, body, handlerFunc)
// 		if msgUpdated {
// 			break
// 		}

// 		for n.NodeIDs()[index] == n.ID() {
// 			index = (index + 1) % len(n.NodeIDs())
// 		}
// 		target = n.NodeIDs()[index]
// 	}

// }

// func fetchMessages() {

// }

func main() {

	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)

	// go func() {
	// 	d := time.NewTicker(500 * time.Millisecond)
	// 	for range d.C {
	// 		var body map[string]any
	// 		mu.RLock()
	// 		body = map[string]any{
	// 			"type":     "timed_broadcast",
	// 			"messages": slices.Collect(maps.Keys(messages)),
	// 		}
	// 		mu.RUnlock()

	// for i := 0; i < 3; {
	// 	randomNode := n.NodeIDs()[rand.Intn(len(n.NodeIDs()))]
	// 	if randomNode == n.ID() {
	// 		continue
	// 	}
	// 	n.Send(randomNode, body)
	// 	i++
	// }
	// 	}
	// }()

	// var nodeID int
	n.Handle("init", func(msg maelstrom.Message) error {
		// strip leading character 'n' from node ID
		// https://fly.io/dist-sys/1/ node id is "n1", "n2", ...
		var err error
		// nodeID, err = strconv.Atoi(n.ID()[1:])
		// if err != nil {
		// 	return err
		// }

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()

		globalCounter, err = kv.ReadInt(ctx, CounterKey)
		if err != nil {
			if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
				globalCounter = 0
			} else {

				return err
			}

		}
		sequenceCounter, err = kv.ReadInt(ctx, SequenceKey)
		if err != nil {
			if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
				sequenceCounter = 0
			} else {

				return err
			}
		}

		untypedBody, err := kv.Read(ctx, LastMessageIDKey)
		if err != nil {
			if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
				lastMessageID = make([]int, len(n.NodeIDs()))
			} else {
				return err
			}
		} else {
			var ok bool
			lastMessageID, ok = untypedBody.([]int)
			if !ok {
				return fmt.Errorf("failed to get lastMessageID")
			}
		}

		untypedBody, err = kv.Read(ctx, DeltaBufferKey)
		if err != nil {
			if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
				deltaBuffer = make([]int, 0)
			} else {
				return err
			}
		} else {
			var ok bool
			deltaBuffer, ok = untypedBody.([]int)
			if !ok {
				return fmt.Errorf("failed to get deltaBuffer")
			}
		}

		announceYouAreBackBuffer := []Msg{}
		globalCounterMu.RLock()
		for index, value := range deltaBuffer {
			announceYouAreBackBuffer = append(announceYouAreBackBuffer, Msg{
				RefCounter: index,
				Value:      value,
			})
		}
		globalCounterMu.RUnlock()

		go forward(announceYouAreBackBuffer, n.ID(), n)

		return nil
	})

	// n.Handle("timed_broadcast", func(msg maelstrom.Message) error {
	// 	var body struct {
	// 		Message []int `json:"messages"`
	// 	}
	// 	err := json.Unmarshal(msg.Body, &body)
	// 	if err != nil {
	// 		return err
	// 	}

	// 	mu.Lock()
	// 	for _, v := range body.Message {
	// 		messages[v] = struct{}{}
	// 	}
	// 	mu.Unlock()

	// 	return nil
	// })

	// n.Handle("fetch", func(msg maelstrom.Message) error {
	// 	var body struct {
	// 		Ref    int    `json:"ref"`
	// 		Target string `json:"targetNode"`
	// 	}

	// 	err := json.Unmarshal(msg.Body, &body)
	// 	if err != nil {
	// 		return err
	// 	}

	// 	delta := 0
	// 	ref := 0
	// 	if n.ID() == body.Target {

	// 		index := body.Ref + 1
	// 		for index < len(deltaBuffer) {
	// 			delta += deltaBuffer[body.Ref]
	// 			ref = index
	// 			index++
	// 		}
	// 	} else {
	// 		n.RPC(body.Target, map[string]any{
	// 			"type":       "fetch",
	// 			"targetNode": body.Target,
	// 			"ref":        body.Ref,
	// 		}, func(msg maelstrom.Message) error {

	// 		})
	// 	}

	// 	return nil
	// })

	n.Handle("gossip", func(msg maelstrom.Message) error {

		var body struct {
			Root string `json:"root"`
			Msgs []Msg  `json:"messages"`
		}

		err := json.Unmarshal(msg.Body, &body)
		if err != nil {
			return err
		}

		inBufferMu.Lock()
		msgSeq := lastMessageID[getNodeID(body.Root)]

		shouldForward := false

		for _, val := range body.Msgs {

			if msgSeq <= val.RefCounter {
				inBuffer[body.Root] = append(inBuffer[body.Root], val)
				shouldForward = true

			}
		}

		if shouldForward {
			go forward(body.Msgs, body.Root, n)
		}

		slices.SortFunc(inBuffer[body.Root], func(a, b Msg) int {
			if a.RefCounter < b.RefCounter {
				return -1
			} else if a.RefCounter == b.RefCounter {
				return 0
			} else {
				return 1
			}
		})

		globalCounterMu.Lock()
		stopIndex := 0
		for _, value := range inBuffer[body.Root] {
			if value.RefCounter != msgSeq {
				break
			}

			globalCounter += value.Value
			msgSeq++
			stopIndex++
		}

		globalCounterMu.Unlock()
		inBuffer[body.Root] = inBuffer[body.Root][stopIndex:]
		lastMessageID[getNodeID(body.Root)] = msgSeq

		inBufferMu.Unlock()

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

		globalCounterMu.Lock()
		globalCounter += body.Delta
		go forward([]Msg{{RefCounter: sequenceCounter, Value: body.Delta}}, n.ID(), n)
		sequenceCounter++
		deltaBuffer = append(deltaBuffer, body.Delta)
		globalCounterMu.Unlock()

		err = put(kv)
		if err != nil {
			return err
		}

		return n.Reply(msg, map[string]any{"type": "add_ok"})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		globalCounterMu.RLock()
		body := map[string]any{
			"type":  "read_ok",
			"value": globalCounter,
		}
		globalCounterMu.RUnlock()
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
