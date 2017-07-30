package gokvdb

import (
	"os"
	"fmt"
)

type LazyStrI64SetDict struct {
	dbName string
	dictName string
	storage *Storage
	internalPager IPager
	keyFactory *SimpleStrI64Factory
	contextByKey map[string]*LazyStrI64SetContext
}

type LazyStrI64SetContext struct {
	pid uint32
	key string
	set *LazyI64Set
	isChanged bool 
}

func (self *LazyStrI64SetDict) ToString() string {
	return fmt.Sprintf("<LazyStrI64SetDict %v>", self.keyFactory.ToString())
}

func (self *LazyStrI64SetContext) ToString() string {
	return fmt.Sprintf("<LazyStrI64SetContext pid=%v key=%v isChanged=%v>", self.pid, self.key, self.isChanged)
}

func (self *LazyStrI64SetDict) _NewContext(pid uint32, key string, data []byte) *LazyStrI64SetContext {
	ctx := new(LazyStrI64SetContext)
	ctx.pid = pid
	ctx.key = key
	ctx.set = NewLazyI64Set(self.internalPager, data)
	ctx.isChanged = false

	return ctx
}

func (self *LazyStrI64SetDict) _LoadContext(pid uint32, key string) *LazyStrI64SetContext {
	
	data, _ := self.internalPager.ReadPayloadData(pid)
	//fmt.Println("_LoadContext", "pid", pid, "data", data)
	ctx := self._NewContext(pid, key, data)
	return ctx
}

type LazyStrI64SetItem struct {
	ctx *LazyStrI64SetContext
	dict *LazyStrI64SetDict
}

func (self *LazyStrI64SetItem) Key() string {
	return self.ctx.key
}

func (self *LazyStrI64SetItem) Values() chan int64 {
	return self.ctx.set.Values()
}

func (self *LazyStrI64SetDict) Get(key string) (LazyStrI64SetItem, bool) {
	//fmt.Println("(self *LazyStrI64SetDict) Get(key string) (LazyStrI64SetItem, bool) {")
	ctx := self._GetOrLoadContext(key)

	if ctx != nil {

		return LazyStrI64SetItem{dict: self, ctx:ctx}, true
	}

	return LazyStrI64SetItem{}, false
}

func (self *LazyStrI64SetDict) _GetOrLoadContext(key string) *LazyStrI64SetContext {
	ctx, ok := self.contextByKey[key]
	if ok {
		return ctx
	}

	_pgId, ok := self.keyFactory.Get(key)	
	//fmt.Println("_GetOrLoadContext _pgId, ok ", _pgId, ok )
	if ok {
		pgId := uint32(_pgId)
		ctx = self._LoadContext(pgId, key)
		return ctx
	}

	return nil
}

func (self *LazyStrI64SetDict) Add(key string, value int64) {

	ctx, ok := self.contextByKey[key]
	
	if !ok {
		//fmt.Println("self.keyFactory.Get(key)..", key)
		_pgId, ok := self.keyFactory.Get(key)
		//fmt.Println("self.keyFactory.Get(key)..",_pgId, ok )
		if ok {
			pgId := uint32(_pgId)
			ctx = self._LoadContext(pgId, key)
		} else {
			pgId := self.internalPager.CreatePageId()
			ctx = self._NewContext(pgId, key, nil)
			ctx.isChanged = true
			self.keyFactory.Set(key, int64(pgId))
		}

		self.contextByKey[key] = ctx
	}

	ctx.set.Add(value)
	ctx.isChanged = true
	
	
}


func (self *LazyStrI64SetDict) Save() {

	for _, ctx := range self.contextByKey {
		if ctx.isChanged {
			ctxData := ctx.set.Save()
			self.internalPager.WritePayloadData(ctx.pid, ctxData)
			//fmt.Println("LazyStrI64SetDict SAVE", ctx.ToString(), "bytes", len(ctxData))
			ctx.isChanged = false
		}
	}

	db, err := self.storage.DB(self.dbName)
	if err != nil {
		fmt.Println("error dbName", self.dbName)
		os.Exit(1)
	}

	keyFactoryData := self.keyFactory.Save()
	pagerData := self.internalPager.Save()

	metaW := NewDataStream()
	metaW.WriteChunk(pagerData)
	metaW.WriteChunk(keyFactoryData)

	db.SetMeta(self.dictName, metaW.ToBytes())
	self.storage.Save()
}


func NewLazyStrI64SetDict(storage *Storage, dbName string, dictName string) *LazyStrI64SetDict {

	self := new(LazyStrI64SetDict)
	self.storage = storage
	self.dbName = dbName
	self.dictName = dictName
	self.contextByKey = make(map[string]*LazyStrI64SetContext)
	
	var pagerData []byte
	var keyData []byte

	db, err := storage.DB(dbName)
	if err != nil {
		fmt.Println("error dbName", dbName)
		os.Exit(1)
	}

	metaData, ok := db.GetMeta(dictName)


	if ok {
		rd := NewDataStreamFromBuffer(metaData)

		pagerData = rd.ReadChunk()
		keyData = rd.ReadChunk()
	}

	fmt.Println("keyData", keyData)


	internalPageSize := uint16(128)

	internalPager := NewInternalPager(storage.pager, internalPageSize, pagerData)
	self.internalPager = internalPager
	self.keyFactory = NewSimpleStrI64Factory(internalPager, keyData)

	fmt.Println("...NewLazyStrI64SetDict", self.ToString(), self.internalPager.ToString())

	return self
}

