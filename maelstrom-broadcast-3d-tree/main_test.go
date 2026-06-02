package main

import (
	"slices"
	"testing"
)


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
			got := children(tt.nodes, tt.root, tt.parent, 2)

			slices.Sort(got)
			want := tt.want
			slices.Sort(want)
			if !slices.Equal(got, want) {
				t.Errorf("children(%v, %q, %q) = %v, want %v", tt.nodes, tt.root, tt.parent, got, want)
			}
		})
	}
}

