package main

import (
	"errors"
	"log"
	"strconv"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// Challenge #2: Unique ID Generation
// https://fly.io/dist-sys/2/
//
// The challenge requires total availability: nodes must generate unique IDs even during network
// partitions, so nodes cannot coordinate. Any scheme where each component of
// the ID is scoped to a single node works: node ID + local counter, UUIDs, or Snowflake IDs.
// Snowflake was chosen for fun.
//
// Solved using: https://en.wikipedia.org/wiki/Snowflake_ID
// 64-bit: 0 | 41-bit timestamp ms since custom epoch | 10-bit node ID | 12-bit sequence number
//
// Uniqueness: timestamp+nodeID pair is unique per millisecond across nodes; sequence number
// disambiguates up to 4096 IDs generated on the same node within the same millisecond.
//
// Custom epoch: RC batch start date 2026-05-18; gives ~69 years before overflow (2^41 ms).
//
// IDs are returned as strings to avoid JS double-precision loss.
//
// Not implemented but required in production:
// - Clock going backwards (NTP slew, leap seconds): refuse to issue IDs until time catches up.
// - Crash recovery: persist lastTimestamp so a restarted node cannot reuse a previous timestamp.

var epochBase = time.Date(2026, 0o5, 18, 0, 0, 0, 0, time.UTC)

func main() {
	n := maelstrom.NewNode()

	var nodeID int64
	n.Handle("init", func(msg maelstrom.Message) error {
		// strip leading character 'n' from node ID
		// https://fly.io/dist-sys/1/ node id is "n1", "n2", ...
		var err error
		nodeID, err = strconv.ParseInt(n.ID()[1:], 10, 64)
		if err != nil {
			return err
		}

		return nil
	})

	lastTimestamp := now()
	var seq uint16
	var mu sync.Mutex
	n.Handle("generate", func(msg maelstrom.Message) error {
		timestamp := now()
		var curSeq uint16
		mu.Lock()
		if timestamp == lastTimestamp {
			seq++
			if seq > 4095 {
				mu.Unlock()
				return errors.New("counter overflow: too many requests in one millisecond")
			}
		} else {
			lastTimestamp = timestamp
			seq = 0
		}
		curSeq = seq
		mu.Unlock()

		id := format(timestamp, nodeID, curSeq)
		body := map[string]any{
			"type": "generate_ok",
			"id":   strconv.FormatInt(id, 10),
		}

		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func now() int64 {
	return time.Since(epochBase).Milliseconds()
}

// format returns a snowflake formatted ID.
// 0|41-bit|10-bit|12-bit
func format(timestamp, nodeID int64, seq uint16) int64 {
	nodeID &= 1023
	nodeID = nodeID << 12

	timestamp &= (1 << 41) - 1
	timestamp <<= 22
	return timestamp | nodeID | (int64(seq) & 4095)
}
