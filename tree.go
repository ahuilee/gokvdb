package gokvdb

import (
	"os"
	"fmt"
)

type BTreeBlobMap struct {
	pager IPager

	lastPageId uint32
	lastNodeId uint32
	rootNodeId uint32
	nodeContextPageId uint32
	pageSize uint32

	nodes map[uint32]*BTreeBlobMapNode
	nodeDataContexts map[uint32]*BTreeBlobMapNodeContext
	//pageContexts map[uint32]*BTreeInternalPageContext

	isChanged bool
}

type BTreeBlobMapNode struct {
	id uint32
	key int64
	dataPageId uint32
	leftNodeId uint32
	rightNodeId uint32
	bt *BTreeBlobMap
}


type BTreeBlobMapNodeContext struct {
	pid uint32
	pageIdByKey map[int64]uint32
	isChanged bool 
	bt *BTreeBlobMap
}


func (bt *BTreeBlobMap) ToString() string {
	return fmt.Sprintf("<BTreeBlobMap lastNodeId=%v rootNodeId=%v nodeContextPageId=%v>", bt.lastNodeId, bt.rootNodeId, bt.nodeContextPageId)
}

func (n *BTreeBlobMapNode) ToString() string {
	return fmt.Sprintf("<BTreeBlobMapNode id=%v key=%v dataPageId=%v leftNodeId=%v rightNodeId=%v>", n.id, n.key, n.dataPageId, n.leftNodeId, n.rightNodeId)
}

func NewBTreeBlobMap(pager IPager, meta []byte) *BTreeBlobMap {

	bt := new(BTreeBlobMap)
	bt.pager = pager
	bt.pageSize = 128

	bt.nodes = make(map[uint32]*BTreeBlobMapNode)
	bt.nodeDataContexts = make(map[uint32]*BTreeBlobMapNodeContext)
//	bt.pageContexts = make(map[uint32]*BTreeInternalPageContext)

	var nodeContextPageId uint32

	if meta != nil && len(meta) > 16 {

		metaR := NewDataStreamFromBuffer(meta)

		bt.lastPageId = metaR.ReadUInt32()
		bt.lastNodeId = metaR.ReadUInt32()
		bt.rootNodeId = metaR.ReadUInt32()
		nodeContextPageId = metaR.ReadUInt32()	
	} else {
		bt.lastPageId = 0
		bt.lastNodeId = 0
		bt.rootNodeId = 0
		nodeContextPageId = 0
	}

	if nodeContextPageId == 0 {
		nodeContextPageId = pager.CreatePageId()
	} else {
		nodesData, err := pager.ReadPayloadData(nodeContextPageId)
		_CheckErr("LOAD BTREE NODES Context", err)
		
		nodesR := NewDataStreamFromBuffer(nodesData)
		nodesCount := nodesR.ReadUInt24()
		var i uint32
		var nodeId uint32
		var nodeKey int64
		var nodeDataPageId uint32
		var nodeLeftId uint32
		var nodeRightId uint32

		for i=0; i<nodesCount; i++ {
			nodeId = nodesR.ReadUInt32()
			nodeKey = int64(nodesR.ReadUInt64())
			nodeDataPageId = nodesR.ReadUInt32()
			nodeLeftId = nodesR.ReadUInt32()
			nodeRightId = nodesR.ReadUInt32()
			
			node := bt._NewNode(nodeId, nodeKey)
			node.dataPageId = nodeDataPageId
			node.leftNodeId = nodeLeftId
			node.rightNodeId = nodeRightId
			bt.nodes[node.id] = node

			//fmt.Println("LOAD", node.ToString())
		}

		//fmt.Println("LOAD NODES", len(bt.nodes))
	}

	bt.nodeContextPageId = nodeContextPageId


	return bt
}

func (bt *BTreeBlobMap) Get(key int64) ([]byte, bool) {

	node := bt._FindNode(key)

	if node != nil {
		ctx := node.GetOrCreateDataContext()

		pageId2, ok := ctx.pageIdByKey[key]

		if ok {

			outputData, err := bt.pager.ReadPayloadData(pageId2)

			if err == nil {
				return outputData, true
			}
		}
	}

	return nil, false
}

func (m *BTreeBlobMap) Set(key int64, value []byte) {

	node := m._InsertNode(key)
	//fmt.Println("BTreeBlobMap Set", "key=", key, node.ToString(), "value bytes", len(value))

	ctx := node.GetOrCreateDataContext()

	//fmt.Println("pageIdByKey", ctx.pageIdByKey)
	pageId2, ok := ctx.pageIdByKey[key]
	//fmt.Println("Tree Set", "key=", key, "pageId=", pageId, ok)

	if !ok {
		pageId2 = m.pager.CreatePageId()
		ctx.pageIdByKey[key] = pageId2
		ctx.isChanged = true
	}

	m.pager.WritePayloadData(pageId2, value)

	m.isChanged = true

}


func (bt *BTreeBlobMap) Save() []byte {


	nodesW := NewDataStream()

	nodesW.WriteUInt24(uint32(len(bt.nodes)))

	for _, node := range bt.nodes {
		nodesW.WriteUInt32(node.id)
		nodesW.WriteUInt64(uint64(node.key))
		nodesW.WriteUInt32(node.dataPageId)
		nodesW.WriteUInt32(node.leftNodeId)
		nodesW.WriteUInt32(node.rightNodeId)
		//fmt.Println("SAVE NODE", node.ToString())
	}

	bt.pager.WritePayloadData(bt.nodeContextPageId, nodesW.ToBytes())

	for dataPid, dataContext := range bt.nodeDataContexts {
		//if true {
		if dataContext.isChanged {
			//fmt.Println(bt.ToString(), "[SAVE DATA Context]", "dataPid=", dataPid, "rows=", len(dataContext.pageIdByKey))

			w := NewDataStream()
			w.WriteUInt24(uint32(len(dataContext.pageIdByKey)))
			for key, pid := range dataContext.pageIdByKey {
				w.WriteUInt64(uint64(key))
				w.WriteUInt32(pid)
			}

			bt.pager.WritePayloadData(dataPid, w.ToBytes())

			dataContext.isChanged = false
		}
	}

	meta := NewDataStream()
	meta.WriteUInt32(bt.lastPageId)
	meta.WriteUInt32(bt.lastNodeId)
	meta.WriteUInt32(bt.rootNodeId)
	meta.WriteUInt32(bt.nodeContextPageId)

	return meta.ToBytes()
}

type BTreeBlobMapItem struct {
	key int64
	pid uint32
	bt *BTreeBlobMap
}

func (i *BTreeBlobMapItem) Key() int64 {
	return i.key
}

func (i *BTreeBlobMapItem) Value() []byte {
	data, err := i.bt.pager.ReadPayloadData(i.pid)
	_CheckErr("DBBTree Items", err)
	return data
}


func (m *BTreeBlobMap) Items() chan BTreeBlobMapItem {
	q := make(chan BTreeBlobMapItem)

	go func(ch chan BTreeBlobMapItem) {
		nodesCount := 0

		for node := range m._Nodes() {
			nodesCount += 1

			ctx := node.GetDataContext()

			if ctx == nil {
				fmt.Println("Items ctx is nil", node.ToString())
			}

			if ctx != nil {
				//fmt.Println("Items ctx", node.ToString(), len(ctx.pageIdByKey))
				/*
				for key, pgId := range ctx.pageIdByKey {			
					item := BTreeInternalItem{key: key, bt: bt, pid: pgId}
					ch <- item
				}*/
			}
		}

		fmt.Println("Items Done nodesCount", nodesCount)

		close(q)
	} (q)

	return q}


func (bt *BTreeBlobMap) _Nodes() chan *BTreeBlobMapNode {

	q := make(chan *BTreeBlobMapNode)

	go func(ch chan *BTreeBlobMapNode) {
		
		rootNode := bt._GetRootNode()
		node := rootNode
		goRight := true
		stack := make([]*BTreeBlobMapNode, 0)

		for {
			if node == nil {
				break
			}
			leftNode := node.GetLeftNode()
			rightNode := node.GetRightNode()

			if goRight && rightNode != nil {
				stack = append(stack, node)
				node = rightNode
			} else {

				ch <- node
				if leftNode != nil {
					node = leftNode
					goRight = true
				} else {
					if len(stack) == 0{
						break
					}

					//fmt.Println("POP STACK", len(stack))

					node = stack[len(stack)-1]
					stack = stack[:len(stack)-1]
					goRight = false

					//fmt.Println("POP STACK DONE", len(stack))

				}
			}
		}

		close(ch)
	} (q)

	return q
}

func (bt *BTreeBlobMap) _FindNode(key int64) *BTreeBlobMapNode {

	findKey := bt._GetBranchKey(key)
	//findKey := key

	var node *BTreeBlobMapNode

	rootNode := bt._GetRootNode()
	node = rootNode

	loopCount := 0

	for {
		if node == nil {
			break
		}

		loopCount += 1

		if node.key == findKey {
			//fmt.Println("_FindNode", node.ToString(), "loopCount", loopCount)
			return node
		}

		//stack = append(stack, node)
		if findKey < node.key {
			node = node.GetLeftNode()
		} else {
			node = node.GetRightNode()
		}

	}
/*
	if len(stack) > 0 {
		return stack[len(stack)-1]
	}*/

	return nil
}

func (bt *BTreeBlobMap) _GetBranchKey(key int64) int64 {

	return int64((key / 4096) * 4096)
}

const (
	BTREE_NODE_DIR_LEFT = 0
	BTREE_NODE_DIR_RIGHT = 1

)

func (bt *BTreeBlobMap) _InsertNode(key int64) *BTreeBlobMapNode {

	findKey := bt._GetBranchKey(key)
	//findKey := key

	var node *BTreeBlobMapNode
	var parent *BTreeBlobMapNode
	var dir byte

	rootNode := bt._GetRootNode()
	node = rootNode

	var nodeStack []*BTreeBlobMapNode
	var dirStack []byte

	loopCount := 0

	for {
		loopCount += 1

		if node == nil {
			node = bt._CreateNode(findKey)
			if dir == BTREE_NODE_DIR_LEFT {
				parent.SetLeftNode(node)
			} else {
				parent.SetRightNode(node)
			}

			bt._DoBalance(nodeStack, dirStack)
			//fmt.Println("_CreateNode", node.ToString())
		}

		if node.key == findKey {
			//fmt.Println(fmt.Sprintf("_InsertNode key=%v loopCount=%v\n", key, loopCount), node.ToString(), "findKey", findKey, )
			return node
		}

		parent = node

		nodeStack = append(nodeStack, node)

		if findKey < node.key {
			node = node.GetLeftNode()
			dir = BTREE_NODE_DIR_LEFT
		} else {
			node = node.GetRightNode()
			dir = BTREE_NODE_DIR_RIGHT
		}
		dirStack = append(dirStack, dir)

	}
}


func (bt* BTreeBlobMap) _DoBalance(nodeStack []*BTreeBlobMapNode, dirStack []byte) {

	balanceCount := 0
	for i:=len(nodeStack)-1; i>=0; i-- {
		node := nodeStack[i]
		leftHeight := bt._CalcHeight(node, BTREE_NODE_DIR_LEFT)
		rightHeight := bt._CalcHeight(node, BTREE_NODE_DIR_RIGHT)
		balance := rightHeight - leftHeight

		//fmt.Printf("_DoBalance balance=%v leftHeight=%v rightHeight=%v\n", balance, leftHeight, rightHeight)

		var newRoot *BTreeBlobMapNode
		if balance < -1 {
			newRoot = bt._RotateRight(node)
			fmt.Println("_DoBalance _RotateRight", node.ToString())

		} else if balance > 1 {
			newRoot = bt._RotateLeft(node)
			fmt.Println("_DoBalance _RotateLeft", node.ToString())
		}

		if newRoot != nil {
			if i > 0 {
				parent := nodeStack[i-1]
				nodeDir := dirStack[i-1]
				if nodeDir == BTREE_NODE_DIR_LEFT {
					parent.SetLeftNode(newRoot)
				} else {
					parent.SetRightNode(newRoot)
				}

			} else {
				bt._SetRootNode(newRoot)
			}

			bt.isChanged = true

			balanceCount += 1
			
		}

		if balanceCount >= 8 {
			break
		}

	}
}


func (bt* BTreeBlobMap) _RotateLeft(node *BTreeBlobMapNode) *BTreeBlobMapNode {

	nodeRight := node.GetRightNode()
	node.SetRightNode(nodeRight.GetLeftNode())
	nodeRight.SetLeftNode(node)

	return nodeRight
}

func (bt* BTreeBlobMap) _RotateRight(node *BTreeBlobMapNode) *BTreeBlobMapNode {
	nodeLeft := node.GetLeftNode()
	node.SetLeftNode(nodeLeft.GetRightNode())
	nodeLeft.SetRightNode(node)

	return nodeLeft
}

func (bt* BTreeBlobMap) _CalcHeight(node *BTreeBlobMapNode, dir byte) int {
	height := 0
	for {
		if node == nil {
			break
		}
		height += 1
		if dir == BTREE_NODE_DIR_LEFT {
			node = node.GetLeftNode()
		} else {
			node = node.GetRightNode()
		}
	}

	return height
}


func (bt *BTreeBlobMap) _CreateNodeId() uint32 {
	nid := bt.lastNodeId + 1
	bt.lastNodeId = nid
	bt.isChanged = true
	return nid
}


func (bt *BTreeBlobMap) _CreateNodeDataContext() *BTreeBlobMapNodeContext {

	pid := bt.pager.CreatePageId()
	dp := bt._NewNodeDataContext(pid)
	bt.nodeDataContexts[pid] = dp

	dp.isChanged = true
	bt.isChanged = true

	return dp
}

func (bt *BTreeBlobMap) _NewNodeDataContext(pid uint32) *BTreeBlobMapNodeContext {
	dp := new(BTreeBlobMapNodeContext)
	dp.pid = pid
	dp.pageIdByKey = make(map[int64]uint32)
	dp.bt = bt
	dp.isChanged = true
	return dp
}


func (bt *BTreeBlobMap) _NewNode(id uint32, key int64) *BTreeBlobMapNode {
	node := new(BTreeBlobMapNode)
	node.id = id
	node.key = key
	node.bt = bt

	return node
}

func (bt *BTreeBlobMap) _CreateNode(key int64) *BTreeBlobMapNode {

	id := bt._CreateNodeId()
	node := bt._NewNode(id, key)
	bt.nodes[id] = node
	bt.isChanged = true

	return node
}


func (bt *BTreeBlobMap) _GetNode(nodeId uint32) *BTreeBlobMapNode {

	node, ok := bt.nodes[nodeId]
	if !ok {

		fmt.Printf("Node is None id=%v\n",  nodeId)
		os.Exit(1)

	}

	return node
}

func (bt* BTreeBlobMap) _GetNodeDataContext(pid uint32) *BTreeBlobMapNodeContext {
	ctx, ok := bt.nodeDataContexts[pid]

	if !ok {
		ctx = bt._NewNodeDataContext(pid)

		pageIdByKey := make(map[int64]uint32)
		data, err := bt.pager.ReadPayloadData(pid)
		if err == nil {

			rd := NewDataStreamFromBuffer(data)
			rowCount := int(rd.ReadUInt24())
			for i:=0; i<rowCount; i++ {
				rowKey := int64(rd.ReadUInt64())
				rowPid := rd.ReadUInt32()

				pageIdByKey[rowKey] = rowPid
			}
		
			
		} else  {

			fmt.Println("_GetDataContext ERRPR !!!!!!", "pid=", pid, err)
			os.Exit(1)
		}

		ctx.pageIdByKey = pageIdByKey
		//fmt.Println("LOAD DATA Context", "pid=", pid, "rows", len(ctx.pageIdByKey))

		bt.nodeDataContexts[pid] = ctx
	}

	return ctx
}

func (bt* BTreeBlobMap) _SetRootNode(node *BTreeBlobMapNode) {

	bt.rootNodeId = node.id
	bt.isChanged = true
}

func (bt *BTreeBlobMap) _GetRootNode() *BTreeBlobMapNode {
	if bt.rootNodeId == 0 {
		rootNode := bt._CreateNode(4096)
		bt.rootNodeId = rootNode.id
		return rootNode
	}

	return bt._GetNode(bt.rootNodeId)
}


func (n *BTreeBlobMapNode) GetLeftNode() *BTreeBlobMapNode {
	if n.leftNodeId > 0 {
		return n.bt._GetNode(n.leftNodeId)
	}

	return nil
}

func (n *BTreeBlobMapNode) GetRightNode() *BTreeBlobMapNode {
	if n.rightNodeId > 0 {
		return n.bt._GetNode(n.rightNodeId)
	}

	return nil
}

func (n *BTreeBlobMapNode) SetLeftNode(node *BTreeBlobMapNode) {
	n.leftNodeId = 0
	if node != nil {
		n.leftNodeId = node.id
	}
}

func (n *BTreeBlobMapNode) SetRightNode(node *BTreeBlobMapNode) {
	n.rightNodeId = 0
	if node != nil {
		n.rightNodeId = node.id
	}
}


func (n *BTreeBlobMapNode) GetDataContext() *BTreeBlobMapNodeContext {
	if n.dataPageId > 0 {
		//fmt.Println(n.ToString(), "GetDataContext", n.dataPageId )

		return n.bt._GetNodeDataContext(n.dataPageId)
	}

	return nil
}

func (n *BTreeBlobMapNode) GetOrCreateDataContext() *BTreeBlobMapNodeContext {

	dp := n.GetDataContext()
	if dp == nil {
		dataContext := n.bt._CreateNodeDataContext()
		n.dataPageId = dataContext.pid
		dp = dataContext
	}

	return dp
}


