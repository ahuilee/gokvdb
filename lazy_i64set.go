
package gokvdb

import (
	//"os"
	"fmt"
	"sort"
)


type I64Array []int64

func (self I64Array) Len() int { return len(self) }
func (self I64Array) Swap(i, j int) { self[i], self[j] = self[j], self[i] }
func (self I64Array) Less(i, j int) bool { return self[i] < self[j] }




/* */

type LazyI64Set struct {
	pager IPager
	treeFactory *BranchI64BTreeFactory
	contextByPageId map[uint32]*LazyI64SetContext
}

type LazyI64SetContext struct {
	pid uint32
	branchKey int64
	data map[int64]byte
	isChanged bool
}

func (self *LazyI64Set) ToString() string {
	return fmt.Sprintf("<LazyI64Set %v>", self.treeFactory.ToString())
}


func (self *LazyI64SetContext) ToString() string {
	return fmt.Sprintf("<LazyI64SetContext branchKey=%v pid=%v rows=%v isChanged=%v>", self.branchKey, self.pid, len(self.data), self.isChanged)
}



func (self *LazyI64Set) Save() []byte {

	//fmt.Println("SAVE...", self.ToString())

	for pid, ctx := range self.contextByPageId {
		if ctx.isChanged {
			ctx.isChanged = false

			ctxW := NewDataStream()
			ctxW.WriteUInt24(uint32(len(ctx.data)))
			for v, _ := range ctx.data {
				ctxW.WriteUInt64(uint64(v))
			}
			ctxData := ctxW.ToBytes()
			self.pager.WritePayloadData(pid, ctxData)

			//fmt.Println("SAVE CTX pid", pid, "bytes", len(ctxData))
		}
	}


	treeMeta := self.treeFactory.Save()

	metaW := NewDataStream()
	metaW.WriteChunk(treeMeta)

	return metaW.ToBytes()
}


func (self *LazyI64Set) Values() chan int64 {
	q := make(chan int64)

	go func(ch chan int64) {


		for item := range self.treeFactory.Items() {

			pageId := uint32(item.Value())
			ctx, ok := self.contextByPageId[pageId]
			if !ok {

				branchKey := item.Key()
				
				ctxData, _ := self.pager.ReadPayloadData(pageId)
				//fmt.Println("_EachContexts LoadContext", ctxData)
				ctx = self.LoadContext(pageId, branchKey, ctxData)
			}

			var vals I64Array//[]int64
			for k, _ := range ctx.data {
				vals = append(vals, k)
			}
			sort.Sort(vals)
			for _, v := range vals {
				ch <- v
			}

		}

		close(ch)
	} (q)

	return q
}

func (self *LazyI64Set) Add(value int64) {
	var pageId uint32

	branchKey := value / 4096

	page := self.treeFactory.GetOrCreatePage(branchKey)	

	var ctxPageId uint32
	var ctx *LazyI64SetContext

	_ctxPageId, ok := page.Get(branchKey)
	if ok {
		ctxPageId = uint32(_ctxPageId)
	} else {
		ctxPageId = self.pager.CreatePageId()
		page.Set(branchKey, int64(ctxPageId))
	
		ctx = self.NewContext(ctxPageId, branchKey)
		ctx.isChanged = true
		self.contextByPageId[ctxPageId] = ctx
	}

	if ctx == nil {
		ctx, ok = self.contextByPageId[ctxPageId]
		if !ok {
			ctxData, _ := self.pager.ReadPayloadData(pageId)
			ctx = self.LoadContext(ctxPageId, branchKey, ctxData)
			self.contextByPageId[ctxPageId] = ctx
		}
	}

	ctx.data[value] = 1
	ctx.isChanged = true

	//fmt.Println("ADD", ctx.ToString())
}


func (self *LazyI64Set) LoadContext(pid uint32, branchKey int64, data []byte) *LazyI64SetContext {
	ctx := self.NewContext(pid, branchKey)

	rd := NewDataStreamFromBuffer(data)
	rowsCount := int(rd.ReadUInt24())
	//fmt.Println("LoadContext rowsCount", rowsCount)
	
	for i:=0; i<rowsCount; i++ {
		v := int64(rd.ReadUInt64())
		ctx.data[v] = 1
	}

	return ctx
}

func (self *LazyI64Set) NewContext(pid uint32, branchKey int64) *LazyI64SetContext {
	ctx := new(LazyI64SetContext)
	ctx.pid = pid
	ctx.branchKey = branchKey
	ctx.data = make(map[int64]byte)
	return ctx
}

func (self *LazyI64Set) ReleaseCache() {

}


func NewLazyI64Set(pager IPager, meta []byte) *LazyI64Set {
	self := new(LazyI64Set)
	self.pager = pager
	self.contextByPageId = make(map[uint32]*LazyI64SetContext)

	var treeFactoryMeta []byte

	if meta != nil {

		rd := NewDataStreamFromBuffer(meta)
		treeFactoryMeta = rd.ReadChunk()
	}
	
	self.treeFactory = NewBranchI64BTreeFactory(pager, treeFactoryMeta, 3)

	return self
}
