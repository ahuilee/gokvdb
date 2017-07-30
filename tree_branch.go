package gokvdb

import (
	//"os"
	"fmt"
	//"sort"
	//"sync"
)

/*
type _RWLock struct {
	lock sync.Mutex
}*/

type BranchI64BTreeFactory struct {
	pager IPager
	rootPageId uint32
	treePageByPageId map[uint32]*BranchI64BTreePage

}

type BranchI64BTreePage struct {
	pid uint32
	tree *I64I64BTreePage
	isChanged bool
}

func (self *BranchI64BTreeFactory) ToString() string {
	return fmt.Sprintf("<BranchI64BTreeFactory rootPageId=%v>", self.rootPageId)
}


func (self *BranchI64BTreePage) ToString() string {
	return fmt.Sprintf("<BranchI64BTreePage pid=%v isChanged=%v>", self.pid, self.isChanged)
}

func (self *BranchI64BTreeFactory) CalcBranchKeys(value int64) []int64 {
	//k3 := value / 8192
	k2 := value / 4096
	k1 := k2 / 4096
	//keys := []int64{k1, k2, k3}
	keys := []int64{k1, k2}

	return keys
}

func (self *BranchI64BTreeFactory) NewTreePage(pid uint32, data []byte) *BranchI64BTreePage {
	treePage := new(BranchI64BTreePage)
	treePage.pid = pid
	treePage.tree = NewI64I64BTreePage(data)
	treePage.isChanged = false

	return treePage
}

func NewBranchI64BTreeFactory(pager IPager, meta []byte) *BranchI64BTreeFactory {
	self := new(BranchI64BTreeFactory)
	self.pager = pager
	self.treePageByPageId = make(map[uint32]*BranchI64BTreePage)

	rootPageId := uint32(0)	

	if meta != nil {
		rd := NewDataStreamFromBuffer(meta)

		rootPageId = rd.ReadUInt32()
	}

	self.rootPageId = rootPageId

	return self
}



func (self *BranchI64BTreeFactory) Items() chan I64I64BTreeItem {

	q := make(chan I64I64BTreeItem)

	go func(ch chan I64I64BTreeItem) {

		root := self.GetRootPage()
		//fmt.Println("Contexts", self.ToString(), root.ToString())

		self._EachItems(root, ch, 0)

		close(ch)
	} (q)

	return q
}

func (self *BranchI64BTreeFactory) LoadTreePage(pid uint32) *BranchI64BTreePage {

	pageData, _ := self.pager.ReadPayloadData(pid)
	treePage := self.NewTreePage(pid, pageData)
	return treePage
}


func (self *BranchI64BTreeFactory) _EachItems(page *BranchI64BTreePage, outCh chan I64I64BTreeItem, depth int) {
	fmt.Println("BEGIN _EachContexts depth", depth, page.ToString())

	if depth >= 2 {
		for item := range page.tree.Items() {
			outCh <- item
		}

		return
	}

	for item := range page.tree.Items() {
		pageId := uint32(item.Value())

		//fmt.Println("_FillValues depth", depth, "pageId", pageId)

		treePage, ok := self.treePageByPageId[pageId]
		if !ok {
			treePage = self.LoadTreePage(pageId)
		}

		//fmt.Println("_EachContexts depth", depth, "treePage", treePage.ToString())

		self._EachItems(treePage, outCh, depth+1)
	}

}

func (self *BranchI64BTreeFactory) GetRootPage() *BranchI64BTreePage {
	if self.rootPageId > 0 {
		page, ok := self.treePageByPageId[self.rootPageId]
		if !ok {
			data, _ := self.pager.ReadPayloadData(self.rootPageId)
			page = self.NewTreePage(self.rootPageId, data)
			page.isChanged = true
			self.treePageByPageId[self.rootPageId] = page
			return page
		}
		return page
	}

	return nil
}


func (self *BranchI64BTreeFactory) GetPage(key int64) *BranchI64BTreePage {

	root := self.GetRootPage()
	if root != nil {
		keys := self.CalcBranchKeys(key)

		page := root

		var pageId uint32

		for i:=0; i<len(keys); i++  {

			k := keys[i]

			var nextPage *BranchI64BTreePage
			_pageId, ok := page.tree.Get(k)

			if !ok {
				return nil
			}
			
			pageId = uint32(_pageId)
			_nextPage, ok := self.treePageByPageId[pageId]
			if ok {
				nextPage = _nextPage
			}		
			if nextPage == nil {
				nextPageData, _ := self.pager.ReadPayloadData(pageId)
				nextPage = self.NewTreePage(pageId, nextPageData)
				self.treePageByPageId[pageId] = nextPage
			}
			page = nextPage
		}

		return page
		
	}

	return nil
}

func (self *BranchI64BTreeFactory) GetOrCreatePage(key int64) *BranchI64BTreePage {

	keys := self.CalcBranchKeys(key)

	root := self.GetOrCreateRootPage()
	page := root

	var pageId uint32

	for i:=0; i<len(keys); i++  {
		k := keys[i]
		//fmt.Println("ADD LOOP i=", i, "k=", k, "page", page.ToString)
		var nextPage *BranchI64BTreePage

		//fmt.Println("page.tree.Get(k)...")

		_pageId, ok := page.tree.Get(k)
		//fmt.Println("page.tree.Get(k)...done")

		if !ok {

			pageId = self.pager.CreatePageId()
			nextPage = self.NewTreePage(pageId, nil)
			nextPage.isChanged = true
			self.treePageByPageId[pageId] = nextPage

			page.tree.Insert(k, int64(pageId))
			page.isChanged = true

			//fmt.Println("CreateTreePage", pageId, nextPage.ToString())

		} else {
			pageId = uint32(_pageId)
			_nextPage, ok := self.treePageByPageId[pageId]
			if ok {
				nextPage = _nextPage
			}
		}

		if nextPage == nil {
			nextPageData, _ := self.pager.ReadPayloadData(pageId)
			nextPage = self.NewTreePage(pageId, nextPageData)
			self.treePageByPageId[pageId] = nextPage
		}

		page = nextPage
	}

	return page
}


func (self *BranchI64BTreeFactory) Save() []byte {

	//fmt.Println("SAVE...", self.ToString())

	for pid, treePage := range self.treePageByPageId {
		//fmt.Println("SAVE...", treePage.ToString())
		if treePage.isChanged {
			treePage.isChanged = false
	
			treeData := treePage.tree.ToBytes()

			//fmt.Println("SAVE TREE pid", pid, "bytes", len(treeData))

			self.pager.WritePayloadData(pid, treeData)
		}

	}

	metaW := NewDataStreamFromBuffer(make([]byte, 32))
	metaW.WriteUInt32(self.rootPageId)

	return metaW.ToBytes()
}


func (self *BranchI64BTreeFactory) GetOrCreateRootPage() *BranchI64BTreePage {

	page := self.GetRootPage()

	if page == nil {

		pageId := self.pager.CreatePageId()

		self.rootPageId = pageId
		page = self.NewTreePage(pageId, nil)
		page.isChanged = true
		self.treePageByPageId[self.rootPageId] = page
	}

	return page
}


func (self *BranchI64BTreePage) Get(key int64) (int64, bool) {

	val, ok := self.tree.Get(key)
	return val, ok
}

func (self *BranchI64BTreePage) Set(key int64, value int64) {

	self.tree.Insert(key, value)
	self.isChanged = true

}
