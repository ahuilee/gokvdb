package gokvdb

import (
	"os"
	"fmt"
	"math"
)

const NON_CLUSTERED_BRANCH uint8 = 1
const NON_CLUSTERED_DATA uint8 = 2
const NON_CLUSTERED_SIZE int = 128

type HashNode struct {
	pid uint32
	mask uint32
	changed bool
	context map[uint32]uint32
}

type NonClusteredContext map[uint64]uint64

type NonClusteredNode struct {
	pid uint32
	nodeType uint8
	depth uint8
	changed bool
	context NonClusteredContext
}

func (n *HashNode) ToString() string {
	return fmt.Sprintf("<HashNode pid=%v mask=%v context=%v>", n.pid, n.mask, len(n.context))
}

func (n *NonClusteredNode) ToString() string {
	return fmt.Sprintf("<NonClusteredNode pid=%v nodeType=%v depth=%v>", n.pid, n.nodeType, n.depth)
}

/* HASH NODE */

func (db *DB) _CreateHashNode(mask uint32) *HashNode {
	n := new(HashNode)
	n.pid = db._CreatePageId()
	n.mask = mask
	n.changed = true
	n.context = make(map[uint32]uint32)
	db.hashNodes[n.pid] = n

	return n
}


func (db *DB) _LoadHashNode(pid uint32) (*HashNode, error) {

	dataBytes, err := db._LoadPayloadPage(pid)
	
	if err != nil {
		fmt.Println("_LoadHashNode _LoadPayloadPage", err)
		return nil, err
	}

	node, err := _Loads(dataBytes, pid, PGTYPE_HASH_NODE)
	if err != nil {
		return nil, err
	}

	return node.(*HashNode), nil
}

func (db *DB) _GetHashNode(pid uint32) (*HashNode, bool) {
	node, ok := db.hashNodes[pid]
	if !ok {
		node, err := db._LoadHashNode(pid)
		if err != nil {
			return nil, false
		}
		
		db.hashNodes[pid] = node
		return node, true		
	}

	return node, ok
}

/* NON CLUSTERED NODE */

func (db *DB) _GetNonClusteredNode(pid uint32) (*NonClusteredNode, bool) {

	node, ok := db.nonClusteredNodes[pid]
	if !ok {

		node := db._LoadNonClusteredNode(pid)
		//fmt.Println("_GetNonClusteredNode _LoadNonClusteredNode", node)
		db.nonClusteredNodes[pid] = node
		return node, true
	}

	return node, ok
}



func (db *DB) _LoadNonClusteredNode(pid uint32) *NonClusteredNode {
	//node.nodeType = NON_CLUSTERED_DATA
	dataBytes, err := db._LoadPayloadPage(pid)
	if err != nil {
		return nil
	}

	pg, err := _Loads(dataBytes, pid, PGTYPE_NON_CLUSTERED_NODE)

	if err == nil {
		return pg.(*NonClusteredNode)
	}

	return nil
}



func (db *DB) _CreateNonClusteredNode(nodeType uint8, depth uint8) *NonClusteredNode {

	pid := db._CreatePageId()
	node := new(NonClusteredNode)
	node.pid = pid
	node.nodeType = nodeType
	node.depth = depth
	node.context = make(NonClusteredContext)
	node.changed = true

	db.nonClusteredNodes[pid] = node

	return node
}




func _CalcNonClusteredBranchKey(key uint64, depth int) uint64 {

	if depth < 1 {
		return key
	}

	chunkSize := uint64(math.Pow(float64(NON_CLUSTERED_SIZE), float64(depth)))
	branchKey := uint64((key / chunkSize) * chunkSize)
	//fmt.Println("_CalcNonClusteredBranchKey", "key=", key, "depth=", depth, "chunkSize=", chunkSize, "branchKey", branchKey)
	return branchKey
}



func (db *DB) _InsertNonClusteredValue(root *NonClusteredNode, key uint64, value uint32) {

	if root == nil {
		fmt.Println("_InsertNonClusteredValue root is nil")
		os.Exit(1)
	}

	//fmt.Println( "_InsertNonClusteredValue", "root", root.ToString(), "key=", key, "value=", value)
	
	node := root
	
	var childNode *NonClusteredNode
	var branchKey uint64
	var depth int

	depth = int(root.depth)

	for {
		if depth < 0 {
			break
		}

		branchKey = _CalcNonClusteredBranchKey(key, depth)

		//fmt.Println("_InsertNonClusteredValue depth", depth, "key", key, "branchKey", branchKey)

		switch node.nodeType {
		case NON_CLUSTERED_BRANCH:
			//fmt.Println("NON_CLUSTERED_BRANCH", depth)
			childNodePageId, ok := node.context[branchKey]

			//fmt.Println("childNodePageId, ok", childNodePageId, ok)
			
			var newChildNode *NonClusteredNode
			if !ok {
				if depth > 1 {
					newChildNode = db._CreateNonClusteredNode(NON_CLUSTERED_BRANCH, uint8(depth - 1))
				} else {
					newChildNode = db._CreateNonClusteredNode(NON_CLUSTERED_DATA, 0)
				}

				node.context[branchKey] = uint64(newChildNode.pid)
				node.changed = true

				childNodePageId = uint64(newChildNode.pid)

			}

			childNode, ok = db._GetNonClusteredNode(uint32(childNodePageId))
			//fmt.Println("_InsertNonClusteredValue", "childNode.pid", childNode.pid, ok)
			
			if newChildNode != nil {
				if newChildNode != childNode {
					fmt.Println("NEW CHILD NODE LOGIC ERROR")
					os.Exit(1)
				}
			}
			if !ok {
				fmt.Println("_InsertNonClusteredValue _GetNonClusteredNode NO OK LOGIC ERROR")
				os.Exit(1)
			}
			node = childNode			
			
		case NON_CLUSTERED_DATA:
			//fmt.Println("INSERT NON_CLUSTERED_DATA", depth)
			if depth != 0 {
				fmt.Println("_InsertNonClusteredValue NON_CLUSTERED_DATA depth is 0 required")
				os.Exit(1)
			}
			node.context[branchKey] = uint64(value)
			node.changed = true
			//fmt.Println("_InsertNonClusteredValue SUCCESS!")
		}

		depth -= 1
	}
}


func (db *DB) _GetNonClusteredValue(root *NonClusteredNode, key uint64) (uint64, bool) {

	//fmt.Println("-----------------------", "_GetNonClusteredValue")

	node := root

	var branchKey uint64
	var depth int

	depth = int(root.depth)

	for {
		if depth < 0 {
			break
		}

		branchKey = _CalcNonClusteredBranchKey(key, depth)

		//fmt.Println("_GetNonClusteredValue depth", depth, "key", key, "branchKey", branchKey)

		switch node.nodeType {
		case NON_CLUSTERED_BRANCH:
			childNodePageId, ok := node.context[branchKey]
			if !ok {
				return 0, false	
			}
			childNode, ok := db._GetNonClusteredNode(uint32(childNodePageId))
			if !ok {
				fmt.Println("_GetNonClusteredValue _GetNonClusteredNode NO OK LOGIC ERROR")
				os.Exit(1)
			}
			node = childNode			
			
		case NON_CLUSTERED_DATA:
			if depth != 0 {
				fmt.Println("_GetNonClusteredValue NON_CLUSTERED_DATA depth is 0 required")
				os.Exit(1)
			}
			val, ok := node.context[branchKey]
			//fmt.Println("NON_CLUSTERED_DATA", node.context, val, ok)
			return val, ok
		}

		depth -= 1

	}

	return 0, false
}

