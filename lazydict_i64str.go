package gokvdb


import (
	"fmt"
	"os"
	"sort"
	//"hash/fnv"
)

type LazyI64StrDict struct {
	dbName string
	dictName string
	storage *Storage
	
	contextByBranchKey map[int64]*LazyI64StrContext
	//bt *BTreeIndex
	internalPager IPager
	keyFactory *BranchI64BTreeFactory
	//bt *BTreeBlobMap
	isChanged bool
}

type LazyI64StrContext struct {
	pid uint32
	branchKey int64
	getValueByKey map[int64]string
	isChanged bool
	dict *LazyI64StrDict
}

func NewI64StrDict(s *Storage, dbName string, dictName string) *LazyI64StrDict {

	self := new(LazyI64StrDict)
	self.dbName = dbName
	self.dictName = dictName
	self.storage = s
	self.contextByBranchKey = make(map[int64]*LazyI64StrContext)

	db, err := s.DB(dbName)
	if err != nil {
		fmt.Println("error dbName", dbName)
		os.Exit(1)
	}

	metaData, ok := db.GetMeta(dictName)

	var internalPagerMeta []byte
	var keyFactoryMeta []byte

	if ok {

		rd := NewDataStreamFromBuffer(metaData)
		internalPagerMeta = rd.ReadChunk()
		keyFactoryMeta = rd.ReadChunk()
		//fmt.Println("NewI64StrDict keyFactoryMeta", keyFactoryMeta)

	} 

	internalPageSize := uint16(128)
	internalPager := NewInternalPager(s.pager, internalPageSize, internalPagerMeta)	

	self.internalPager = internalPager
	self.keyFactory = NewBranchI64BTreeFactory(internalPager, keyFactoryMeta, 3)

	return self
}

func (d *LazyI64StrDict) ToString() string {
	return fmt.Sprintf("<LazyI64StrDict db=%v name=%v>", d.dbName, d.dictName)
}

func (c *LazyI64StrContext) ToString() string {
	return fmt.Sprintf("<LazyI64StrContext pid=%v branchKey=%v>", c.pid, c.branchKey)
}


func (d *LazyI64StrDict) _GetBranchKey(key int64) int64 {
	return key / 4096
}

type LazyI64StrDictItem struct {
	key int64
	value string
}

func (i *LazyI64StrDictItem) Key() int64 {
	return i.key
}

func (i *LazyI64StrDictItem) Value() string {
	return i.value
}

func (self *LazyI64StrDict) Items() chan LazyI64StrDictItem {
	q := make(chan LazyI64StrDictItem)
	go func(ch chan LazyI64StrDictItem) {


		for _item := range self.keyFactory.Items() {

			branchKey := _item.Key()
			ctxPageId := uint32(_item.Value())

			ctx, ok := self.contextByBranchKey[branchKey]
			if !ok {
				ctx = self._ReadContext(ctxPageId, branchKey)
			}

			var keys I64Array

			for k, _ := range ctx.getValueByKey {
				keys = append(keys, k)
			}

			sort.Sort(keys)

			for _, k := range keys {
				v, _ := ctx.getValueByKey[k]
				ch <- LazyI64StrDictItem{key: k, value: v}
			}
		}

		close(ch)
	}(q)

	return q
}

func (self *LazyI64StrDict) Set(key int64, value string) {
	branchKey := self._GetBranchKey(key)
	ctx := self._GetContextByBranchKey(branchKey)

	if ctx == nil {
		ctxPageId := self.internalPager.CreatePageId()
		
		page := self.keyFactory.GetOrCreatePage(branchKey)	
		page.Set(branchKey, int64(ctxPageId))

		ctx = self._NewContext(ctxPageId, branchKey)
		ctx.isChanged = true
		self.contextByBranchKey[branchKey] = ctx
	}

	//fmt.Println(d.ToString(), "SET", key, value, ctx.ToString())

	ctx.getValueByKey[key] = value
	ctx.isChanged = true
}

func (self *LazyI64StrDict) Get(key int64) (string, bool) {
	//bt := d._GetBt()
	branchKey := self._GetBranchKey(key)
	ctx := self._GetContextByBranchKey(branchKey)
	if ctx != nil {
		val, ok := ctx.getValueByKey[key]
		if ok {
			return val, true
		}
	}

	return "", false
}

func (self *LazyI64StrDict) ReleaseCache() {
	var keys []int64
	for key, ctx := range self.contextByBranchKey {
		if !ctx.isChanged {
			keys = append(keys, key)
		}
	}

	for _, key := range keys {
		ctx, _ := self.contextByBranchKey[key]
		fmt.Println("[ReleaseCache]", ctx.ToString())
		delete(self.contextByBranchKey, key)
		ctx = nil
	}
}

func (self *LazyI64StrDict) Save(commit bool) {
	//fmt.Println("Save", d.ToString())
	//isChanged := false

	for _, ctx := range self.contextByBranchKey {
		if ctx.isChanged {

			w := NewDataStream()
			w.WriteUInt24(uint32(len(ctx.getValueByKey)))

			for k, v := range ctx.getValueByKey {
				w.WriteUInt64(uint64(k))
				w.WriteChunk([]byte(v))
				//fmt.Println("SAVE CONTEXT", k, v)
			}

			ctxData := w.ToBytes()
			self.internalPager.WritePayloadData(ctx.pid, ctxData)

			ctx.isChanged = false
			//isChanged = true
			//fmt.Println("Save", ctx.ToString(), "bytes", len(ctxData))
		}
	}


	db, err := self.storage.DB(self.dbName)
	if err != nil {
		fmt.Println("error dbName", self.dbName)
		os.Exit(1)
	}

	keyMeta := self.keyFactory.Save()
	internalPagerMeta := self.internalPager.Save()

	metaW := NewDataStream()
	metaW.WriteChunk(internalPagerMeta)
	metaW.WriteChunk(keyMeta)

	metaBytes := metaW.ToBytes()

	db.SetMeta(self.dictName, metaBytes)

	if commit {
		self.storage.Save()
	}
}

func (d *LazyI64StrDict) _NewContext(pid uint32, branchKey int64) *LazyI64StrContext {
	ctx := new(LazyI64StrContext)
	ctx.pid = pid
	ctx.branchKey = branchKey
	ctx.getValueByKey = make(map[int64]string)
	ctx.isChanged = false

	return ctx
}


func (self *LazyI64StrDict) _ReadContext(pid uint32, branchKey int64) *LazyI64StrContext {

	ctx := self._NewContext(pid, branchKey)

	data, _ := self.internalPager.ReadPayloadData(pid)

			
	rd := NewDataStreamFromBuffer(data)
	rowsCount := int(rd.ReadUInt24())
	for i:=0; i<rowsCount; i++ {
		key := int64(rd.ReadUInt64())
		valChunk := rd.ReadChunk()
		ctx.getValueByKey[key] = string(valChunk)
	}
	return ctx
}

func (self *LazyI64StrDict) _GetContextByBranchKey(branchKey int64) *LazyI64StrContext {

	ctx, ok := self.contextByBranchKey[branchKey]
	if !ok {
		page := self.keyFactory.GetPage(branchKey)	
		
		if page != nil {
			_ctxPageId, ok := page.Get(branchKey)
			if ok {
				ctxPageId := uint32(_ctxPageId)
				ctx = self._ReadContext(ctxPageId, branchKey)
				self.contextByBranchKey[branchKey] = ctx

				return ctx
			}
		}
	}

	return ctx

}
