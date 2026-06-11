package main

import (
	"encoding/json"
	"log"
	"maps"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var (
	KeyVals          = map[string][]int{}
	keyValMu         sync.RWMutex
	offsetMu         sync.RWMutex
	committedOffsets = map[string]int{}
)

func main() {

	n := maelstrom.NewNode()
	// kv := maelstrom.NewSeqKV(n)

	// n.Handle("init", func(msg maelstrom.Message) error {
	// 	mu.Lock()
	// 	defer mu.Unlock()
	// 	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	// 	defer cancel()

	// 	return nil
	// })

	n.Handle("send", func(msg maelstrom.Message) error {

		var body struct {
			Key string `json:"key"`
			Msg int    `json:"msg"`
		}

		err := json.Unmarshal(msg.Body, &body)
		if err != nil {
			return err
		}

		keyValMu.Lock()
		valList, ok := KeyVals[body.Key]
		if !ok {
			valList = make([]int, 0)
		}

		offset := len(valList)
		valList = append(valList, body.Msg)
		KeyVals[body.Key] = valList
		keyValMu.Unlock()

		return n.Reply(msg, map[string]any{
			"type":   "send_ok",
			"offset": offset,
		})

	})

	n.Handle("poll", func(msg maelstrom.Message) error {

		var body struct {
			Offsets map[string]int `json:"offsets"`
		}

		err := json.Unmarshal(msg.Body, &body)
		if err != nil {
			return err
		}

		response := map[string]any{
			"type": "poll_ok",
		}

		msgs := map[string][][]int{}

		keyValMu.RLock()

		for key, value := range body.Offsets {

			for i := value; i <= len(KeyVals[key])-1; i++ {
				msg := []int{i, KeyVals[key][i]}

				msgs[key] = append(msgs[key], msg)
			}
		}
		keyValMu.RUnlock()

		response["msgs"] = msgs

		return n.Reply(msg, response)

	})

	n.Handle("commit_offsets", func(msg maelstrom.Message) error {

		var body struct {
			Offsets map[string]int `json:"offsets"`
		}

		err := json.Unmarshal(msg.Body, &body)
		if err != nil {
			return err
		}
		offsetMu.Lock()
		maps.Copy(committedOffsets, body.Offsets)
		offsetMu.Unlock()

		return n.Reply(msg, map[string]any{
			"type": "commit_offsets_ok",
		})
	})

	n.Handle("list_committed_offsets", func(msg maelstrom.Message) error {

		var body struct {
			Keys []string `json:"keys"`
		}

		err := json.Unmarshal(msg.Body, &body)
		if err != nil {
			return err
		}

		responseMap := map[string]int{}

		offsetMu.RLock()
		for _, key := range body.Keys {
			responseMap[key] = committedOffsets[key]
		}
		offsetMu.RUnlock()

		return n.Reply(msg, map[string]any{
			"type":    "list_committed_offsets_ok",
			"offsets": responseMap,
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
