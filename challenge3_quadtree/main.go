package main

import (
	// "context"
	"encoding/json"
	"fmt"
	"log"
	"maps"
	"math/rand"
	"slices"
	"strconv"
	"sync"
	"time"

	// "time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var (
	mu        sync.RWMutex
	messages  = map[int]struct{}{}
	topology  map[string][]string
	outbuffer = []int{}
)

// func repeat(n *maelstrom.Node, message map[string]any, skipNodes []string) {
// 	for len(skipNodes) < len(n.NodeIDs()) {
// 		randomNode := n.NodeIDs()[rand.Intn(len(n.NodeIDs()))]
// 		if slices.Contains(skipNodes, randomNode) {
// 			continue
// 		}

// 		ctx, f := context.WithDeadline(context.Background(), time.Now().Add(5*time.Second))
// 		defer f()
// 		reply, err := n.SyncRPC(ctx, randomNode, message)
// 		if err != nil {
// 			skipNodes = append(skipNodes, randomNode)
// 			continue
// 		}

// 		var replyBody struct {
// 			Added bool `json:"added"`
// 		}
// 		err = json.Unmarshal(reply.Body, &replyBody)
// 		if err != nil {
// 			log.Println("reply struct is missing added field")
// 			return
// 		}

// 		if replyBody.Added {
// 			return
// 		} else {
// 			skipNodes = append(skipNodes, randomNode)
// 			continue
// 		}

// 	}
// }

// func periodicFlush(){
// 	for{

// 	}
// }t

// https://fly.io/dist-sys/3b/

func fly(root int, n *maelstrom.Node) {

	var msgCpy []int

	mu.RLock()
	for index := max(0, len(outbuffer)-16); index < len(outbuffer); index++ {
		msgCpy = append(msgCpy, outbuffer[index])
	}
	mu.RUnlock()
	selfID, err := strconv.Atoi(n.ID()[1:])
	if err != nil {
		panic(err)
	}

	msg := map[string]any{
		"type":     "gossip",
		"messages": msgCpy,
		"root":     root,
	}

	totalNodes := len(n.NodeIDs())

	for i := 1; i <= 4; i++ {
		calculatedID := ((selfID-root)%totalNodes + totalNodes) % totalNodes
		calculatedID = (calculatedID * 4) + i
		if calculatedID < totalNodes {
			calculatedID = (calculatedID + root) % totalNodes
			dest := fmt.Sprintf("n%d", calculatedID)
			err := n.Send(dest, msg)
			if err != nil {
				log.Printf("error occurred while sending to destID: %s, from source: %s, msg: %+v", dest, n.ID(), msg)
			}
		}
	}

}

func main() {

	n := maelstrom.NewNode()

	go func() {
		d := time.NewTicker(500 * time.Millisecond)
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
				n.Send(randomNode, body)
				i++
			}
		}
	}()

	var nodeID int
	n.Handle("init", func(msg maelstrom.Message) error {
		// strip leading character 'n' from node ID
		// https://fly.io/dist-sys/1/ node id is "n1", "n2", ...
		var err error
		nodeID, err = strconv.Atoi(n.ID()[1:])
		if err != nil {
			return err
		}

		return nil
	})

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

	n.Handle("gossip", func(msg maelstrom.Message) error {

		var body struct {
			Messages []int `json:"messages"`
			Root     int   `json:"root"`
		}

		err := json.Unmarshal(msg.Body, &body)
		if err != nil {
			return err
		}

		mu.Lock()
		for _, key := range body.Messages {
			_, ok := messages[key]
			if !ok {
				outbuffer = append(outbuffer, key)
			}
			messages[key] = struct{}{}

		}
		mu.Unlock()
		fly(body.Root, n)

		return nil
	})

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		// This message requests that a value be broadcast out to all nodes in the cluster.
		// The value is always an integer and it is unique for each message from Maelstrom.
		var body struct {
			Message   int    `json:"message"`
			Checklist uint32 `json:"checklist"`
		}
		err := json.Unmarshal(msg.Body, &body)
		if err != nil {
			return err
		}

		// mu.Lock()
		// messages[body.Message] = struct{}{}
		// mu.Unlock()
		// go repeat(n, map[string]any{
		// 	"type":    "broadcast",
		// 	"message": body.Message,
		// }, []string{n.ID(), msg.Src})

		// if msg.Src[0] == 'c' {
		// 	body.Checklist = 0
		// }
		// intNodeID, err := strconv.Atoi(n.ID()[1:])
		// if err != nil {
		// 	return err
		// }

		// body.Checklist |= 1 << (intNodeID)

		// index := 0
		// dir := rand.Intn(2)

		// if dir == 0 {
		// 	index = len(n.NodeIDs())
		// 	dir = -1
		// }

		// counter := 0

		// for ; index < len(n.NodeIDs()) && index >= 0; index += dir {
		// 	if (body.Checklist & (1 << index)) == 0 {
		// 		destNode := fmt.Sprintf("n%d", index)
		// err := n.Send(destNode, map[string]any{
		// 	"type":      "broadcast",
		// 	"message":   body.Message,
		// 	"checklist": body.Checklist,
		// })
		// 		if err != nil {
		// 			log.Printf("unable to send msg: %d, to dest: %s\n", body.Message, destNode)
		// 			continue
		// 		}
		// 		counter++
		// 	}

		// 	if counter == 3 {
		// 		break
		// 	}
		// }

		mu.Lock()
		_, ok := messages[body.Message]
		if !ok {
			outbuffer = append(outbuffer, body.Message)
		}
		messages[body.Message] = struct{}{}
		mu.Unlock()

		go fly(nodeID, n)

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
