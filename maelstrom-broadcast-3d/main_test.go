package main

import (
	"slices"
	"testing"
)

func TestSpanningTree(t *testing.T) {
	type tc struct {
		graph map[string][]string
		root  string
		want  map[string][]string
	}
	tests := map[string]tc{
		"SingleNode": {
			graph: map[string][]string{"n0": {}},
			root:  "n0",
			want:  map[string][]string{},
		},
		"LinearChain": {
			// n0 -- n1 -- n2
			graph: map[string][]string{
				"n0": {"n1"},
				"n1": {"n0", "n2"},
				"n2": {"n1"},
			},
			root: "n0",
			want: map[string][]string{
				"n0": {"n1"},
				"n1": {"n2"},
			},
		},
		"FullyConnectedTriangle": {
			// n0 -- n1, n0 -- n2, n1 -- n2
			graph: map[string][]string{
				"n0": {"n1", "n2"},
				"n1": {"n0", "n2"},
				"n2": {"n0", "n1"},
			},
			root: "n0",
			// BFS from n0: visits n1, n2 as children of n0; the n1-n2 edge is dropped
			want: map[string][]string{
				"n0": {"n1", "n2"},
			},
		},
		"StarTopology": {
			// n0 is center, connects to n1..n4
			graph: map[string][]string{
				"n0": {"n1", "n2", "n3", "n4"},
				"n1": {"n0"},
				"n2": {"n0"},
				"n3": {"n0"},
				"n4": {"n0"},
			},
			root: "n0",
			want: map[string][]string{
				"n0": {"n1", "n2", "n3", "n4"},
			},
		},
		"Grid3x3": {
			// n0 -- n1 -- n2
			//  |     |     |
			// n3 -- n4 -- n5
			//  |     |     |
			// n6 -- n7 -- n8
			graph: map[string][]string{
				"n0": {"n1", "n3"},
				"n1": {"n0", "n2", "n4"},
				"n2": {"n1", "n5"},
				"n3": {"n0", "n4", "n6"},
				"n4": {"n1", "n3", "n5", "n7"},
				"n5": {"n2", "n4", "n8"},
				"n6": {"n3", "n7"},
				"n7": {"n4", "n6", "n8"},
				"n8": {"n5", "n7"},
			},
			root: "n0",
			// BFS from n0: level1={n1,n3}, level2={n2,n4,n6}, level3={n5,n7}, level4={n8}
			want: map[string][]string{
				"n0": {"n1", "n3"},
				"n1": {"n2", "n4"},
				"n2": {"n5"},
				"n3": {"n6"},
				"n4": {"n7"},
				"n5": {"n8"},
			},
		},
		"Grid3x3CenterRoot": {
			// n0 -- n1 -- n2
			//  |     |     |
			// n3 -- n4 -- n5
			//  |     |     |
			// n6 -- n7 -- n8
			graph: map[string][]string{
				"n0": {"n1", "n3"},
				"n1": {"n0", "n2", "n4"},
				"n2": {"n1", "n5"},
				"n3": {"n0", "n4", "n6"},
				"n4": {"n1", "n3", "n5", "n7"},
				"n5": {"n2", "n4", "n8"},
				"n6": {"n3", "n7"},
				"n7": {"n4", "n6", "n8"},
				"n8": {"n5", "n7"},
			},
			root: "n4",
			// BFS from n4: level1={n1,n3,n5,n7}
			// n1 discovers n0,n2; n3 discovers n6 (n0 already seen); n5 discovers n8 (n2 already seen); n7 discovers nothing new
			want: map[string][]string{
				"n4": {"n1", "n3", "n5", "n7"},
				"n1": {"n0", "n2"},
				"n3": {"n6"},
				"n5": {"n8"},
			},
		},
		"GridWithShortcuts": {
			// n0 -- n1 -- n2
			//  |         |
			// n3 ------- n4
			graph: map[string][]string{
				"n0": {"n1", "n3"},
				"n1": {"n0", "n2"},
				"n2": {"n1", "n4"},
				"n3": {"n0", "n4"},
				"n4": {"n2", "n3"},
			},
			root: "n0",
			// BFS from n0: level1={n1,n3}, n1 discovers n2, n3 discovers n4; n2-n4 cross-edge dropped
			want: map[string][]string{
				"n0": {"n1", "n3"},
				"n1": {"n2"},
				"n3": {"n4"},
			},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			got := spanningTree(tt.graph, tt.root)

			for node, wantNeighbors := range tt.want {
				gotNeighbors, ok := got[node]
				if !ok {
					t.Errorf("spanningTree() missing node %q", node)
					continue
				}
				slices.Sort(gotNeighbors)
				slices.Sort(wantNeighbors)
				if !slices.Equal(gotNeighbors, wantNeighbors) {
					t.Errorf("spanningTree() node %q: got neighbors %v, want %v", node, gotNeighbors, wantNeighbors)
				}
			}

			assertIsSpanningTree(t, tt.graph, got, tt.root)
		})
	}
}

func TestChildren(t *testing.T) {
	type tc struct {
		nodes  []string
		root   string
		parent string
		want   []string
	}
	tests := map[string]tc{
		"RootOfOneNode": {
			nodes:  []string{"n0"},
			root:   "n0",
			parent: "n0",
			want:   nil,
		},
		"RootOf3Nodes": {
			// binary tree:  n0
			//              /  \
			//            n1    n2
			nodes:  []string{"n0", "n1", "n2"},
			root:   "n0",
			parent: "n0",
			want:   []string{"n1", "n2"},
		},
		"LeftChildOf3Nodes": {
			nodes:  []string{"n0", "n1", "n2"},
			root:   "n0",
			parent: "n1",
			want:   nil,
		},
		"RootOf7Nodes": {
			// binary tree:      n0
			//                  /  \
			//                n1    n2
			//               / \   / \
			//             n3  n4 n5  n6
			nodes:  []string{"n0", "n1", "n2", "n3", "n4", "n5", "n6"},
			root:   "n0",
			parent: "n0",
			want:   []string{"n1", "n2"},
		},
		"LeftChildOf7Nodes": {
			nodes:  []string{"n0", "n1", "n2", "n3", "n4", "n5", "n6"},
			root:   "n0",
			parent: "n1",
			want:   []string{"n3", "n4"},
		},
		"RightChildOf7Nodes": {
			nodes:  []string{"n0", "n1", "n2", "n3", "n4", "n5", "n6"},
			root:   "n0",
			parent: "n2",
			want:   []string{"n5", "n6"},
		},
		"LeafOf7Nodes": {
			nodes:  []string{"n0", "n1", "n2", "n3", "n4", "n5", "n6"},
			root:   "n0",
			parent: "n3",
			want:   nil,
		},
		// root not at index 0: children wrap around the slice end
		"RootNotAtIndex0": {
			// binary tree rooted at n2 (idx=2) over 5 nodes:
			//         n2
			//        /  \
			//      n3    n4
			//     / \
			//   n0   n1   (wraps around)
			nodes:  []string{"n0", "n1", "n2", "n3", "n4"},
			root:   "n2",
			parent: "n2",
			want:   []string{"n3", "n4"},
		},
		"ChildrenWrapAroundSlice": {
			nodes:  []string{"n0", "n1", "n2", "n3", "n4"},
			root:   "n2",
			parent: "n3",
			want:   []string{"n0", "n1"},
		},
		"RootAtEndOf3Nodes": {
			// binary tree rooted at n2 (idx=2), children wrap around: n0, n1
			nodes:  []string{"n0", "n1", "n2"},
			root:   "n2",
			parent: "n2",
			want:   []string{"n0", "n1"},
		},
		"OnlyOneChildWhenTreeNotFull": {
			// binary tree:   n0
			//               /  \
			//             n1    n2
			//            /
			//          n3
			// n1 has only one child because logical child 4 >= len(4)
			nodes:  []string{"n0", "n1", "n2", "n3"},
			root:   "n0",
			parent: "n1",
			want:   []string{"n3"},
		},
		"LeafWhoseChildWrapsToNonRoot": {
			// root=n3 (idx=3), len=5: logical positions are n3=0,n4=1,n0=2,n1=3,n2=4
			// n1 is at logical 3, a leaf; its logical child 7 wraps to physical (7+3)%5=0="n0", not rootIdx
			nodes:  []string{"n0", "n1", "n2", "n3", "n4"},
			root:   "n3",
			parent: "n1",
			want:   nil,
		},
		"LeafWhenChildIdxWrapsToRoot": {
			// n0 (idx=0), root=n2 (idx=2): logical childIdx should wrap to 2=rootIdx, so n0 is a leaf
			nodes:  []string{"n0", "n1", "n2"},
			root:   "n2",
			parent: "n0",
			want:   nil,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			got := children(tt.nodes, tt.root, tt.parent)

			slices.Sort(got)
			want := tt.want
			slices.Sort(want)
			if !slices.Equal(got, want) {
				t.Errorf("children(%v, %q, %q) = %v, want %v", tt.nodes, tt.root, tt.parent, got, want)
			}
		})
	}
}

// assertIsSpanningTree verifies structural invariants: all nodes reachable from root,
// no cycles, and edge count == node count - 1.
func assertIsSpanningTree(t *testing.T, graph, tree map[string][]string, root string) {
	t.Helper()

	edgeCount := 0
	for _, neighbors := range tree {
		edgeCount += len(neighbors)
	}
	if edgeCount != len(graph)-1 {
		t.Errorf("spanning tree has %d edges, want %d (nodes-1)", edgeCount, len(graph)-1)
	}

	visited := map[string]bool{root: true}
	queue := []string{root}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for _, nb := range tree[cur] {
			if visited[nb] {
				t.Errorf("cycle detected: node %q visited twice", nb)
				return
			}
			visited[nb] = true
			queue = append(queue, nb)
		}
	}

	for node := range graph {
		if !visited[node] {
			t.Errorf("node %q not reachable from root %q in spanning tree", node, root)
		}
	}
}
