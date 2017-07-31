package gokvdb


import (
	"os"
	"fmt"
)

type LazyI64I64SetDict struct {

	storage *Storage
	dbName string
	ixName string

	internalPager IPager
	treeFactory *BranchI64BTreeFactory
	ctxByKey map[int64]*LazyI64I64SetContext
}

type LazyI64I64SetContext struct {
	pid uint32
	key int64
	set *LazyI64Set
	isChanged bool 
}

func (self *LazyI64I64SetDict) ToString() string {
	return fmt.Sprintf("<LazyI64I64SetDict db=%v name=%v %v>", self.dbName, self.ixName, self.treeFactory.ToString())
}

func (self *LazyI64I64SetContext) ToString() string {
	return fmt.Sprintf("<LazyI64I64SetContext key=%v pid=%v>", self.key, self.pid)
}

func (self *LazyI64I64SetDict) ReleaseCache() {

	var keys []int64

	for key, ctx := range self.ctxByKey {
		if !ctx.isChanged {
			keys = append(keys, key)
		}
	}

	for _, key := range keys {
		ctx, _ := self.ctxByKey[key]
		fmt.Println("ReleaseCache", ctx.ToString())
		delete(self.ctxByKey, key)
		ctx = nil		
	}
}

func (self *LazyI64I64SetDict) Save(commit bool) {

	for _, ctx := range self.ctxByKey {
		if ctx.isChanged {
			ctx.isChanged = false
			ctxData := ctx.set.Save()
			self.internalPager.WritePayloadData(ctx.pid, ctxData)

			//fmt.Println("LazyI64I64SetDict SAVE ctx", ctx.ToString(), "len", len(ctxData))
		}
	}

	db, err := self.storage.DB(self.dbName)
	if err != nil {
		fmt.Println("error dbName", self.dbName)
		os.Exit(1)
	}

	treeFactoryMeta := self.treeFactory.Save()
	internalPagerMeta := self.internalPager.Save()

	metaW := NewDataStream()
	metaW.WriteChunk(internalPagerMeta)
	metaW.WriteChunk(treeFactoryMeta)

	metaBytes := metaW.ToBytes()

	db.SetMeta(self.ixName, metaBytes)

	if commit {
		self.storage.Save()
	}

}

func NewLazyI64I64SetDict(storage *Storage, dbName string, ixName string) *LazyI64I64SetDict {

	self := new(LazyI64I64SetDict)
	self.storage = storage
	self.dbName = dbName
	self.ixName = ixName
	self.ctxByKey = make(map[int64]*LazyI64I64SetContext)

	db, err := storage.DB(dbName)
	if err != nil {
		fmt.Println("error dbName", dbName)
		os.Exit(1)
	}

	metaData, ok := db.GetMeta(ixName)

	var internalPagerMeta []byte
	var treeFactoryMeta []byte
	if ok {

		rd := NewDataStreamFromBuffer(metaData)
		internalPagerMeta = rd.ReadChunk()
		treeFactoryMeta = rd.ReadChunk()
	} 

	internalPageSize := uint16(128)
	internalPager := NewInternalPager(storage.pager, internalPageSize, internalPagerMeta)

	self.internalPager = internalPager
	self.treeFactory = NewBranchI64BTreeFactory(internalPager, treeFactoryMeta, 3)
	return self
}

func (self *LazyI64I64SetDict) NewContext(pid uint32, key int64, data []byte) *LazyI64I64SetContext {
	ctx := new(LazyI64I64SetContext)
	ctx.pid = pid
	ctx.key = key
	ctx.set = NewLazyI64Set(self.internalPager, data)
	ctx.isChanged = false
	return ctx
}

type LazyI64I64SetItem struct {
	key int64
	ctxPageId uint32
	i64i64set *LazyI64I64SetDict
}

func (self *LazyI64I64SetItem) Key() int64 {
	return self.key
}

func (self *LazyI64I64SetItem) Values() chan int64 {

	ctx, ok := self.i64i64set.ctxByKey[self.key]
	if !ok {
		ctx = self.i64i64set.LoadContext(self.ctxPageId, self.key)
	}
	
	return ctx.set.Values()
}

func (self *LazyI64I64SetDict) LoadContext(ctxPageId uint32, key int64) *LazyI64I64SetContext {

	setData, _ := self.internalPager.ReadPayloadData(ctxPageId)
	ctx := self.NewContext(ctxPageId, key, setData)

	return ctx
}

func (self *LazyI64I64SetDict) Items() chan LazyI64I64SetItem {

	q := make(chan LazyI64I64SetItem)

	go func(ch chan LazyI64I64SetItem) {
		
		for _item := range self.treeFactory.Items() {

			key := _item.Key()
			ctxPageId := uint32(_item.Value())

			item := LazyI64I64SetItem{key: key, ctxPageId:ctxPageId, i64i64set:self}
			ch <- item
		}
		close(ch)

	}(q)


	return q
}

func (self *LazyI64I64SetDict) Get(key int64) (LazyI64I64SetItem, bool) {

	page := self.treeFactory.GetPage(key)	
	if page != nil {
		_ctxPageId, ok := page.Get(key)
		if ok {
			ctxPageId := uint32(_ctxPageId)
			item := LazyI64I64SetItem{key: key, ctxPageId:ctxPageId, i64i64set:self}
			return item, true
		}
	}

	return LazyI64I64SetItem{}, false
}



func (self *LazyI64I64SetDict) Add(key int64, value int64) {
	var ctx *LazyI64I64SetContext
	var ok bool
	ctx, ok = self.ctxByKey[key]

	//fmt.Println("LazyI64I64Set Add", "key=", key, ctx, ok)

	if !ok {
		page := self.treeFactory.GetOrCreatePage(key)	

		var ctxPageId uint32

		_ctxPageId, ok := page.Get(key)
		if ok {
			ctxPageId = uint32(_ctxPageId)
			ctx = self.LoadContext(ctxPageId, key)
		} else {
			ctxPageId = self.internalPager.CreatePageId()
			page.Set(key, int64(ctxPageId))
		
			ctx = self.NewContext(ctxPageId, key, nil)
			ctx.isChanged = true
			//ctx.isChanged = true
		}
		
		self.ctxByKey[key] = ctx
	}

	ctx.set.Add(value)
	ctx.isChanged = true

}
