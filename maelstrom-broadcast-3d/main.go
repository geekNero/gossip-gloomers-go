package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"maps"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var (
	mu       sync.RWMutex
	messages = map[int]struct{}{}
	trees    map[string]map[string][]string // spanning trees
)

// https://fly.io/dist-sys/3d/
//
// TODO am I too slow due to my chosen tree? or do I have a mistake in my broadcast impl?
// :stable-latencies {0 13,
//
//	0.5 472,
//	0.95 686,
//	0.99 770,
//	1 799},
func main() {
	n := maelstrom.NewNode()
	trees = make(map[string]map[string][]string)

	// TODO add anti-entropy loop to handle partition

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

		// tree, ok := trees[root]
		// if !ok {
		// 	panic("got broadcast from node " + root + " for which we do not have a tree")
		// }
		//
		// nodes := tree[n.ID()]

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

		// storing it before sending ok
		mu.Lock()
		messages[body.Message] = struct{}{}
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
			"messages": slices.Collect(maps.Keys(messages)),
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

		// mu.Lock()
		// for node := range body.Topology {
		// 	trees[node] = spanningTree(body.Topology, node)
		// }
		// mu.Unlock()

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

// TODO godoc that this depends on this invariant
// NodeIDs returns a list of all node IDs in the cluster. This list include the
// local node ID and is the same order across all nodes. Only valid after "init"
// message has been received.
func children(nodes []string, root, parent string) []string {
	rootIdx := slices.Index(nodes, root)
	if rootIdx == -1 {
		panic(fmt.Errorf("root %q not found in list of nodes %s", root, nodes))
	}
	parentIdx := slices.Index(nodes, parent)
	if parentIdx == -1 {
		panic(fmt.Errorf("parent %q not found in list of nodes %s", parent, nodes))
	}
	branch := 2
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
