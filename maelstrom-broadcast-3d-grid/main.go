package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var (
	mu       sync.RWMutex
	messages []int
	trees    map[string]map[string][]string // spanning trees using 2d grid
)

// https://fly.io/dist-sys/3d/
// This implementation uses the suggested grid topology. A spanning tree is built from it so the
// number of messages needed for the broadcast is minimal.

func main() {
	n := maelstrom.NewNode()
	messages = make([]int, 100)
	trees = make(map[string]map[string][]string)

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
		tree, ok := trees[root]
		if !ok {
			panic("got broadcast from node " + root + " for which we do not have a tree")
		}

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

		// storing it before sending ok
		mu.Lock()
		messages = append(messages, body.Message)
		mu.Unlock()

		if msg.Src[0] == 'c' { // only reply to clients as forwarding is fire-and-forget
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
			"messages": messages,
		}
		mu.RUnlock()
		return n.Reply(msg, body)
	})

	// according to the node logs the init message is sent, after which the topology message is
	// sent. Only after topology_ok is received do broadcasts start so we build our spanning trees
	// here to not add latency to the broadcast/read operations.
	n.Handle("topology", func(msg maelstrom.Message) error {
		var body struct {
			Topology map[string][]string `json:"topology"`
		}
		err := json.Unmarshal(msg.Body, &body)
		if err != nil {
			return err
		}
		mu.Lock()
		for node := range body.Topology {
			trees[node] = spanningTree(body.Topology, node)
		}
		mu.Unlock()

		// uncomment to debug the spanning trees visually
		// writeDotFiles(n.ID(), body.Topology, trees)

		reply := map[string]any{
			"type": "topology_ok",
		}
		return n.Reply(msg, reply)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

// TODO make clear what the limitations are and capture a results.edn
// :stable-latencies {0 13,
//
//	0.5 472,
//	0.95 686,
//	0.99 770,
//	1 799},

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

// writeDotFiles writes trees/<nodeID>/nN.dot for every root in allTrees.
// Each file shows the full topology with the spanning-tree edges highlighted red.
func writeDotFiles(nodeID string, topo map[string][]string, allTrees map[string]map[string][]string) {
	dir := filepath.Join("/home/ivo/code/gossip-gloomers-go/maelstrom-broadcast-3d/trees", nodeID)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		log.Printf("writeDotFiles: mkdir %s: %v", dir, err)
		return
	}

	// collect undirected topology edges
	type edge struct{ a, b string }
	canonical := func(u, v string) edge {
		if u > v {
			u, v = v, u
		}
		return edge{u, v}
	}
	topoEdges := map[edge]struct{}{}
	for src, dsts := range topo {
		for _, dst := range dsts {
			topoEdges[canonical(src, dst)] = struct{}{}
		}
	}

	nodePos := func(n string) string {
		idx, _ := strconv.Atoi(n[1:])
		col := idx % 5
		y := 4 - idx/5
		return fmt.Sprintf("%d,%d", col, y)
	}

	for root, tree := range allTrees {
		treeEdges := map[edge]struct{}{}
		for src, dsts := range tree {
			for _, dst := range dsts {
				treeEdges[canonical(src, dst)] = struct{}{}
			}
		}

		path := filepath.Join(dir, root+".dot")
		f, err := os.Create(path)
		if err != nil {
			log.Printf("writeDotFiles: create %s: %v", path, err)
			continue
		}

		w := bufio.NewWriter(f)
		var werr error
		line := func(format string, args ...any) {
			if werr == nil {
				_, werr = fmt.Fprintf(w, format+"\n", args...)
			}
		}
		line("graph %s {", root)
		line("    layout=neato")
		line("    node [shape=circle width=0.5 fixedsize=true]")
		line("")
		for i := range 25 {
			n := fmt.Sprintf("n%d", i)
			line("    %s [pos=%q]", n, nodePos(n)+"!")
		}
		line("")
		for e := range topoEdges {
			if _, inTree := treeEdges[e]; inTree {
				line("    %s -- %s [color=red penwidth=2]", e.a, e.b)
			} else {
				line("    %s -- %s", e.a, e.b)
			}
		}
		line("}")
		if werr == nil {
			werr = w.Flush()
		}
		if werr != nil {
			log.Printf("writeDotFiles: write %s: %v", path, werr)
		}
		if err := f.Close(); err != nil {
			log.Printf("writeDotFiles: close %s: %v", path, err)
		}
	}
}
