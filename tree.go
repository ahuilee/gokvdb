package gokvdb

import (
	"os"
	"fmt"
	"sync"
)


type II64I64BTree interface {
	GetRootNode() II64I64BTreeNode
	SetRootNode(node II64I64BTreeNode)
}

type II64I64BTreeNode interface {

	GetKey() int64

	GetValue() int64
	SetValue(value int64)

	GetLeftNode() II64I64BTreeNode
	SetLeftNode(node II64I64BTreeNode)

	GetRightNode() II64I64BTreeNode
	SetRightNode(node II64I64BTreeNode)

	ToString() string
}



type I64I64BTreePage struct {
	lastNodeId uint32
	rootNodeId uint32
	nodeById map[uint32]*I64I64BTreePageNode
}

type I64I64BTreePageNode struct {
	id uint32
	key int64
	value int64
	leftNodeId uint32
	rightNodeId uint32

	tree *I64I64BTreePage
}

func (self *I64I64BTreePage) GetRootNode() II64I64BTreeNode {
	if self.rootNodeId > 0 {
		node, ok := self.nodeById[self.rootNodeId]
		if ok {
			return node
		}
	}
	return nil
}


func (self *I64I64BTreePage) SetRootNode(node II64I64BTreeNode) {
	self.rootNodeId = 0

	if node != nil {
		_node := node.(*I64I64BTreePageNode)
		self.rootNodeId = _node.id
	}
}

func (self *I64I64BTreePage) NewNode(id uint32, key int64, value int64) *I64I64BTreePageNode {
	node := new(I64I64BTreePageNode)
	node.id = id
	node.tree = self
	node.key = key
	node.value = value
	node.leftNodeId = 0
	node.rightNodeId = 0
	return node
}

func (self *I64I64BTreePage) CreateNode(key int64, value int64) *I64I64BTreePageNode {

	nodeId := self.lastNodeId + 1
	self.lastNodeId = nodeId

	node := self.NewNode(nodeId, key, value)
	self.nodeById[nodeId] = node
	return node
}

func (self *I64I64BTreePage) Count() int {
	return len(self.nodeById)
}

type I64I64BTreeItem struct {
	key int64
	value int64
}

func (self *I64I64BTreeItem) Key() int64 {
	return self.key
}

func (self *I64I64BTreeItem) Value() int64 {
	return self.value
}

func (self *I64I64BTreePage) Items() chan I64I64BTreeItem {
	q := make(chan I64I64BTreeItem)
	go func(ch chan I64I64BTreeItem) {

		for node := range TakeII64I64BTreeNodes(self) {
			_node := node.(*I64I64BTreePageNode)
			item := I64I64BTreeItem{key:_node.key, value: _node.value}
			ch <- item
		}

		close(ch)
	} (q)
	return q
}

func (self *I64I64BTreePage) Get(key int64) (int64, bool) {

	rootNode := self.GetRootNode()
	node := rootNode

	loopCount := 0

	for {
		loopCount += 1
		if node == nil {
			break
		}

		nodeKey := node.GetKey()

		if loopCount > 24 {
			fmt.Println("too many loop", loopCount, self.ToString())
		}

		if nodeKey == key {
			return node.GetValue(), true
		}

		if key < nodeKey {
			node = node.GetLeftNode()
		} else {
			node = node.GetRightNode()
		}
	}

	return 0, false
}

func (self *I64I64BTreePage) Insert(key int64, value int64) {
	rootNode := self.GetRootNode()
	if rootNode == nil {
		rootNode = self.CreateNode(key, value)
		self.SetRootNode(rootNode)
		return
	}

	var node II64I64BTreeNode
	var parent II64I64BTreeNode
	var dir byte

	node = rootNode

	var nodeStack []II64I64BTreeNode
	var dirStack []byte

	loopCount := 0

	for {
		loopCount += 1

		if node == nil {
			node = self.CreateNode(key, value)
			if dir == BTREE_NODE_DIR_LEFT {
				parent.SetLeftNode(node)
			} else {
				parent.SetRightNode(node)
			}

			//fmt.Println("CreateNode", node.ToString())
			II64I64BTree_DoBalance(self, nodeStack, dirStack)
		}

		nodeKey := node.GetKey()

		if nodeKey == key {
			node.SetValue(value)
			//fmt.Println(fmt.Sprintf("_InsertNode key=%v loopCount=%v\n", key, loopCount), node.ToString(), "findKey", findKey, )
			return
		}

		parent = node

		nodeStack = append(nodeStack, node)

		if key < nodeKey {
			node = node.GetLeftNode()
			dir = BTREE_NODE_DIR_LEFT
		} else {
			node = node.GetRightNode()
			dir = BTREE_NODE_DIR_RIGHT
		}
		dirStack = append(dirStack, dir)

	}

}

func (self *I64I64BTreePage) ToString() string {
	return fmt.Sprintf("<I64I64BTreePage rootNodeId=%v>", self.rootNodeId)
}

func (self *I64I64BTreePage) ToBytes() []byte {

	w := NewDataStream()

	w.WriteUInt32(uint32(self.lastNodeId))
	w.WriteUInt32(uint32(self.rootNodeId))

	w.WriteUInt32(uint32(len(self.nodeById)))
	for _, node := range self.nodeById {
		w.WriteUInt32(uint32(node.id))
		w.WriteUInt32(uint32(node.leftNodeId))
		w.WriteUInt32(uint32(node.rightNodeId))
		w.WriteUInt64(uint64(node.key))
		w.WriteUInt64(uint64(node.value))
	}

	return w.ToBytes()
}

func NewI64I64BTreePage(data []byte) *I64I64BTreePage {

	tree := new(I64I64BTreePage)
	tree.nodeById = make(map[uint32]*I64I64BTreePageNode)

	lastNodeId := uint32(0)
	rootNodeId := uint32(0)

	if data != nil {

		rd := NewDataStreamFromBuffer(data)
		lastNodeId = rd.ReadUInt32()
		rootNodeId = rd.ReadUInt32()
		nodeCount := rd.ReadUInt32()

		var i uint32

		var nodeId uint32
		var leftNodeId uint32
		var rightNodeId uint32
		var key int64
		var value int64

		for i=0; i<nodeCount; i++ {
			nodeId = rd.ReadUInt32()
			leftNodeId = rd.ReadUInt32()
			rightNodeId = rd.ReadUInt32()
			key = int64(rd.ReadUInt64())
			value = int64(rd.ReadUInt64())

			node := tree.NewNode(nodeId, key, value)
			node.leftNodeId = leftNodeId
			node.rightNodeId = rightNodeId

			tree.nodeById[nodeId] = node

			//fmt.Println("LOAD", node)
		}
	}

	fmt.Println("NewI64I64BTreePage nodes", len(tree.nodeById))

	tree.lastNodeId = lastNodeId
	tree.rootNodeId = rootNodeId

	return tree
}


func (self *I64I64BTreePageNode) GetKey() int64 {
	return self.key
}

func (self *I64I64BTreePageNode) SetValue(value int64) {
	self.value = value
}

func (self *I64I64BTreePageNode) GetValue() int64 {
	return self.value
}

func (self *I64I64BTreePageNode) GetLeftNode() II64I64BTreeNode {
	//fmt.Println("GetLeftNode", self.ToString())
	if self.leftNodeId > 0 {
		node, ok := self.tree.nodeById[self.leftNodeId]
		if ok {
			return node
		}
	}
	return nil
}

func (self *I64I64BTreePageNode) GetRightNode() II64I64BTreeNode {

	if self.rightNodeId > 0 {
		node, ok := self.tree.nodeById[self.rightNodeId]
		if ok {
			return node
		}
	}
	return nil
}

func (self *I64I64BTreePageNode) SetLeftNode(node II64I64BTreeNode) {
	self.leftNodeId = 0
	if node != nil {
		_node := node.(*I64I64BTreePageNode)
		self.leftNodeId = _node.id
	}
}

func (self *I64I64BTreePageNode) SetRightNode(node II64I64BTreeNode) {
	self.rightNodeId = 0
	if node != nil {
		_node := node.(*I64I64BTreePageNode)
		self.rightNodeId = _node.id
	}

}

func (self *I64I64BTreePageNode) ToString() string {
	return fmt.Sprintf("<LazyBTreePageNode id=%v key=%v value=%v left=%v right=%v>", self.id, self.key, self.value, self.leftNodeId, self.rightNodeId)
}

/* BTree */





func TakeII64I64BTreeNodes(tree II64I64BTree) chan II64I64BTreeNode {

	q := make(chan II64I64BTreeNode)

	go func(chOutNodes chan II64I64BTreeNode) {
		rootNode := tree.GetRootNode()
		node := rootNode
		goLeft := true
		stack := NewLazyList()

		for {
			if node == nil {
				break
			}
			leftNode := node.GetLeftNode()
			rightNode := node.GetRightNode()
			if goLeft && leftNode != nil {
				stack.Append(node)
				
				node = leftNode
			} else {
				chOutNodes <- node
				if rightNode != nil {
					node = rightNode
					goLeft = true

				} else {
					if stack.Len() == 0 {
						break
					}

					node = stack.Pop().(II64I64BTreeNode)
					goLeft = false
				}

			}
		}

		close(chOutNodes)
	}(q)
	return q
}


/* ALV */
func II64I64BTree_DoBalance(tree II64I64BTree, nodeStack []II64I64BTreeNode, dirStack []byte) {

	//fmt.Println("ALVDoBalance", "nodeStack", nodeStack)

	balanceCount := 0
	for i:=len(nodeStack)-1; i>=0; i-- {
		node := nodeStack[i]
		//fmt.Println("ALVDoBalance", node.ToString())
		leftHeight := ALVCalcHeight(node, BTREE_NODE_DIR_LEFT)
		rightHeight := ALVCalcHeight(node, BTREE_NODE_DIR_RIGHT)
		balance := rightHeight - leftHeight

		//fmt.Printf("_DoBalance balance=%v leftHeight=%v rightHeight=%v\n", balance, leftHeight, rightHeight)

		var newRoot II64I64BTreeNode
		if balance < -1 {
			newRoot = ALVRotateRight(node)
			//fmt.Println("_DoBalance _RotateRight", node.ToString())

		} else if balance > 1 {
			newRoot = ALVRotateLeft(node)
			//fmt.Println("_DoBalance _RotateLeft", node.ToString())
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
				tree.SetRootNode(newRoot)
			}

			//bt.isChanged = true

			balanceCount += 1
			
		}

		if balanceCount >= 8 {
			break
		}

	}
}


func ALVRotateLeft(node II64I64BTreeNode) II64I64BTreeNode {

	nodeRight := node.GetRightNode()
	node.SetRightNode(nodeRight.GetLeftNode())
	nodeRight.SetLeftNode(node)

	return nodeRight
}

func ALVRotateRight(node II64I64BTreeNode) II64I64BTreeNode {
	nodeLeft := node.GetLeftNode()
	node.SetLeftNode(nodeLeft.GetRightNode())
	nodeLeft.SetRightNode(node)

	return nodeLeft
}

func ALVCalcHeight(node II64I64BTreeNode, dir byte) int {
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




/**/

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
	rwlock sync.Mutex
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

	if meta != nil && len(meta) >= 16 {

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
		nodesCount := nodesR.ReadUInt32()
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

	nodesW.WriteUInt32(uint32(len(bt.nodes)))

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
			w.WriteUInt32(uint32(len(dataContext.pageIdByKey)))
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
	i.bt.rwlock.Lock()
	defer i.bt.rwlock.Unlock()
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
				for key, pgId := range ctx.pageIdByKey {			
					item := BTreeBlobMapItem{key: key, bt: m, pid: pgId}
					ch <- item
				}
			}
		}

		fmt.Println("Items Done nodesCount", nodesCount)

		close(ch)
	} (q)

	return q
}


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
			//fmt.Println("_DoBalance _RotateRight", node.ToString())

		} else if balance > 1 {
			newRoot = bt._RotateLeft(node)
			//fmt.Println("_DoBalance _RotateLeft", node.ToString())
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
		bt.rwlock.Lock()
		defer bt.rwlock.Unlock()
		ctx = bt._NewNodeDataContext(pid)

		pageIdByKey := make(map[int64]uint32)
		data, err := bt.pager.ReadPayloadData(pid)
		if err == nil {

			rd := NewDataStreamFromBuffer(data)
			rowCount := int(rd.ReadUInt32())
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

