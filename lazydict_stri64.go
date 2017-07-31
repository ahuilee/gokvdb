package gokvdb


import (
	"os"
	"fmt"
	"hash/fnv"
	"sync"
)

const (
	LAZYSTRI64_DATA = 1
	LAZYSTRI64_BRANCH = 2
)

type SimpleStrI64Factory struct {

	lastContextId uint32
	rootContextId uint32
	pageIdByContextId map[uint32]uint32
	contextById map[uint32]*SimpleStrI64Context
	
	pager IPager
	
	splitCount int
	rwlock sync.Mutex
}

type SimpleStrI64Context struct {
	id uint32
	pid uint32
	ctxType byte
	depth byte
	valueByKey map[string]int64
	childContextIdByBranchKey map[int32]uint32

	isChanged bool
	dict *SimpleStrI64Factory
}

func NewSimpleStrI64Factory(pager IPager, data []byte) *SimpleStrI64Factory {

	self := new(SimpleStrI64Factory)
	self.pager = pager
	self.contextById = make(map[uint32]*SimpleStrI64Context)
	self.pageIdByContextId = make(map[uint32]uint32)
	self.splitCount = 0
	
	var lastContextId uint32
	var rootContextId uint32

	//fmt.Println("NewSimpleStrI64Factory", data)
	if data != nil {


		rd := NewDataStreamFromBuffer(data)

		lastContextId = rd.ReadUInt32()
		rootContextId = rd.ReadUInt32()

		//internalPagerMeta = rd.ReadChunk()

		rowsCount := int(rd.ReadUInt32())

		for i:=0; i<rowsCount; i++ {
			ctxId := rd.ReadUInt32()
			pgId := rd.ReadUInt32()
			self.pageIdByContextId[ctxId] = pgId	
			//fmt.Println("NewStrI64Dict LOAD item", "ctxId", ctxId, "pid", pgId)
		}

	}

	self.lastContextId = lastContextId
	self.rootContextId = rootContextId

	fmt.Println("NewSimpleStrI64Factory", self.ToString())

	return self
}

func (self *SimpleStrI64Factory) ToString() string {
	return fmt.Sprintf("<SimpleStrI64Factory lastContextId=%v rootContextId=%v>", self.lastContextId, self.rootContextId)
}

type SimpleStrI64Item struct {
	key string
	value int64
}

func (i *SimpleStrI64Item) Key() string {
	return i.key
}

func (i *SimpleStrI64Item) Value() int64 {
	return i.value
}

func (d *SimpleStrI64Factory) Items() chan SimpleStrI64Item {

	q := make(chan SimpleStrI64Item)

	go func(ch chan SimpleStrI64Item) {

		root := d._GetRoot()
		
		root.TakeItems(ch)

		close(ch)
	} (q)

	return q
}

func (d *SimpleStrI64Factory) Set(key string, value int64) {
	root := d._GetRoot()
	root.Set(key, value)
}

func (d *SimpleStrI64Factory) Get(key string) (int64, bool) {
	root := d._GetRoot()
	//fmt.Println("SimpleStrI64Factory root", root)	
	val, ok := root.Get(key)
	return val, ok
}


func (self *SimpleStrI64Factory) ReleaseCache() {

	var keys []uint32

	for key, ctx := range self.contextById {
		if !ctx.isChanged {
			keys = append(keys, key)
		}
	}

	for _, key := range keys {
		ctx, _ := self.contextById[key]
		fmt.Println("[ReleaseCache]", ctx.ToString())
		delete(self.contextById, key)
		ctx = nil
	}

}

func (d *SimpleStrI64Factory) Save() []byte {

	d.rwlock.Lock()
	defer d.rwlock.Unlock()

	for _, ctx := range d.contextById {
		if ctx.isChanged {
			//
			ctx.isChanged = false

			w := NewDataStream()
			w.WriteUInt8(ctx.ctxType)
			w.WriteUInt8(ctx.depth)

			switch ctx.ctxType {
			case LAZYSTRI64_DATA:
				w.WriteUInt24(uint32(len(ctx.valueByKey)))

				for k, v := range ctx.valueByKey {
					w.WriteHStr(k)
					w.WriteUInt64(uint64(v))
				}

			case LAZYSTRI64_BRANCH:
				w.WriteUInt24(uint32(len(ctx.childContextIdByBranchKey)))

				for k, v := range ctx.childContextIdByBranchKey {
					w.WriteUInt32(uint32(k))
					w.WriteUInt32(v)
				}
			}

			ctxData := w.ToBytes()

			//bt.Set(int64(ctxId), ctxData)
			d.pager.WritePayloadData(ctx.pid, ctxData)

			//fmt.Println("LazyStrI64Dict SAVE ctxId=", ctxId, "bytes", len(ctxData), "rows", len( ctx.valueByKey))
		}
	}


	metaW := NewDataStream()

	metaW.WriteUInt32(d.lastContextId) 
	metaW.WriteUInt32(d.rootContextId) 

	//internalPagerMeta := d.internalPager.Save()
	//metaW.WriteChunk(internalPagerMeta)

	metaW.WriteUInt32(uint32(len(d.pageIdByContextId)))

	for ctxId, pgId := range d.pageIdByContextId {
		metaW.WriteUInt32(ctxId)
		metaW.WriteUInt32(pgId)
	}

	d.splitCount = 0

	return metaW.ToBytes()
}



func (d *SimpleStrI64Factory) _GetRoot() *SimpleStrI64Context {
	if d.rootContextId == 0 {
		root := d._CreateContext(LAZYSTRI64_DATA, 0)
		d.rootContextId = root.id
		return root
	}

	return d._GetContextById(d.rootContextId)
}

func (self *SimpleStrI64Factory) _LoadContext(id uint32, pid uint32) *SimpleStrI64Context {
	self.rwlock.Lock()
	defer self.rwlock.Unlock()

	data, _ := self.pager.ReadPayloadData(pid)
	rd := NewDataStreamFromBuffer(data)

	ctxType := rd.ReadUInt8()
	depth := rd.ReadUInt8()

	ctx := self._NewContext(id, pid, ctxType, depth)	

	var rowsCount int
	rowsCount = int(rd.ReadUInt24())
	//fmt.Println("_GetContext", ctx.ToString(), "rowsCount", rowsCount)
	switch ctx.ctxType {
	case LAZYSTRI64_DATA:

		for i:=0; i<rowsCount; i++ {
			k := rd.ReadHStr()
			v := int64(rd.ReadUInt64())
			ctx.valueByKey[k] = v
		}

	case LAZYSTRI64_BRANCH:

		for i:=0; i<rowsCount; i++ {
			k := int32(rd.ReadUInt32())
			v := rd.ReadUInt32()
			ctx.childContextIdByBranchKey[k] = v
		}
	}

	return ctx
}

func (self *SimpleStrI64Factory) _GetContextById(id uint32) *SimpleStrI64Context {
	ctx, ok := self.contextById[id]

	if !ok {
		pid, ok := self.pageIdByContextId[id]
		if !ok {
			fmt.Printf("[ERROR] no contextId=%v pid=%v\n", id, pid)
			os.Exit(1)
		}		
		
		ctx = self._LoadContext(id, pid)
		self.contextById[id] = ctx
	}

	return ctx
}

func (d *SimpleStrI64Factory) _NewContext(id uint32, pid uint32, ctxType byte, depth byte) *SimpleStrI64Context {

	ctx := new(SimpleStrI64Context)
	ctx.id = id
	ctx.pid = pid
	ctx.ctxType = ctxType
	ctx.depth = depth
	ctx.dict = d
	ctx.isChanged = false
	ctx.valueByKey = make(map[string]int64)
	ctx.childContextIdByBranchKey = make(map[int32]uint32)

	return ctx
}

func (d *SimpleStrI64Factory) _CreateContext(ctxType byte, depth byte) *SimpleStrI64Context {
	d.rwlock.Lock()
	defer d.rwlock.Unlock()

	id := d.lastContextId + 1
	d.lastContextId = id

	pid := d.pager.CreatePageId()

	ctx := d._NewContext(id, pid, ctxType, depth)	
	ctx.isChanged = true
	d.pageIdByContextId[id] = pid
	d.contextById[id] = ctx

	return ctx
}

func _GetBranchSize(depth byte) int32 {

	switch depth {
	case 0:
		return 4096*4096
	}

	return 4096
}

func (c *SimpleStrI64Context) GetChildContext(key string) *SimpleStrI64Context {

	hashKey := _HashString(key)
	branchSize := _GetBranchSize(c.depth)
	branchKey := (hashKey / branchSize) * branchSize

	return c.GetChildContextByBranchKey(branchKey)
}

func (c *SimpleStrI64Context) GetChildContextByBranchKey(branchKey int32) *SimpleStrI64Context {

	childCtxId, ok := c.childContextIdByBranchKey[branchKey]
	if ok {
		return c.dict._GetContextById(childCtxId)
	}

	return nil
}

func (c *SimpleStrI64Context) GetOrCreateChildContext(key string) *SimpleStrI64Context {
	hashKey := _HashString(key)
	branchSize := _GetBranchSize(c.depth)
	branchKey := (hashKey / branchSize) * branchSize

	ctx := c.GetChildContextByBranchKey(branchKey)

	if ctx == nil {
		ctx = c.dict._CreateContext(LAZYSTRI64_DATA, c.depth + 1)
		c.childContextIdByBranchKey[branchKey] = ctx.id
		c.isChanged = true
	}
	
	return ctx
}

func (c *SimpleStrI64Context) ToString() string {
	return fmt.Sprintf("<SimpleStrI64Context id=%v ctxType=%v depth=%v>", c.id, c.ctxType, c.depth)
}


func (c *SimpleStrI64Context) TakeItems(q chan SimpleStrI64Item) {

	if c.ctxType == LAZYSTRI64_BRANCH {

		for _, ctxId := range c.childContextIdByBranchKey {

			childChildCtx := c.dict._GetContextById(ctxId)
			childChildCtx.TakeItems(q)

		}

		return
	}

	for k, v := range c.valueByKey {
		q <- SimpleStrI64Item{key: k, value: v}
	}

}

func (c *SimpleStrI64Context) Get(key string) (int64, bool) {
	//fmt.Println(c.ToString(), "Get", key)
	if c.ctxType == LAZYSTRI64_BRANCH {

		branchCtx := c.GetChildContext(key)
		if branchCtx == nil {
			return 0, false
		}

		return branchCtx.Get(key)
	}

	val, ok := c.valueByKey[key]

	return val, ok
}

func (c *SimpleStrI64Context) Set(key string, value int64) {

	//fmt.Println("[SET]", c.ToString(), key, value)

	if c.ctxType == LAZYSTRI64_BRANCH {

		branchCtx := c.GetOrCreateChildContext(key)

		branchCtx.Set(key, value)
		return
	}

	if c.ctxType != LAZYSTRI64_DATA {
		fmt.Println("c.ctxType != LAZYSTRI64_DATA")
		os.Exit(1)
	}
	
	c.valueByKey[key] = value
	c.isChanged = true

	if c.dict.splitCount < 8 {
		if c.depth < 2 && len(c.valueByKey) > 8192 {
			c.ctxType = LAZYSTRI64_BRANCH

			for k, v := range c.valueByKey {
				//fmt.Println("[SPLIT]", c.ToString(), k, v)
				c.Set(k, v)
			}

			c.valueByKey = nil
			c.isChanged = true
			c.dict.splitCount += 1			
		}
	}
}



func _HashString(key string) int32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int32(h.Sum32())
}



/* */

type LazyStrI64Dict struct {
	dbName string
	dictName string
	storage *Storage
	internalPager IPager
	stri64Factory *SimpleStrI64Factory
}

func NewStrI64Dict(s *Storage, dbName string, dictName string) *LazyStrI64Dict {

	dict := new(LazyStrI64Dict)
	dict.storage = s
	dict.dbName = dbName
	dict.dictName = dictName
	
	var internalPagerMeta []byte
	var factoryMeta []byte

	db, err := s.DB(dbName)
	if err != nil {
		fmt.Println("error dbName", dbName)
		os.Exit(1)
	}

	metaData, ok := db.GetMeta(dictName)

	if ok {
		rd := NewDataStreamFromBuffer(metaData)

		internalPagerMeta = rd.ReadChunk()
		factoryMeta = rd.ReadChunk()
	}


	internalPageSize := uint16(128)

	internalPager := NewInternalPager(s.pager, internalPageSize, internalPagerMeta)
	dict.internalPager = internalPager
	dict.stri64Factory = NewSimpleStrI64Factory(internalPager, factoryMeta)

	fmt.Println("NewStrI64Dict", dict.ToString())

	return dict
}

func (self *LazyStrI64Dict) ReleaseCache() {
	self.stri64Factory.ReleaseCache()
}

func (self *LazyStrI64Dict) Save(commit bool) {


	db := self._GetDB()

	factoryMeta := self.stri64Factory.Save()
	internalPagerMeta := self.internalPager.Save()

	//fmt.Println("LazyStrI64Dict Save factoryMeta", factoryMeta)

	metaW := NewDataStream()
	metaW.WriteChunk(internalPagerMeta)
	metaW.WriteChunk(factoryMeta)

	db.SetMeta(self.dictName, metaW.ToBytes())

	if commit {
		self.storage.Save()
	}
	 
}


func (self *LazyStrI64Dict) Set(key string, value int64) {
	self.stri64Factory.Set(key, value)
}

func (self *LazyStrI64Dict) Get(key string) (int64, bool) {
	return self.stri64Factory.Get(key)
}

func (self *LazyStrI64Dict) Items() chan SimpleStrI64Item {
	return self.stri64Factory.Items()
}



func (d *LazyStrI64Dict) _GetDB() *DBContext {

	db, err := d.storage.DB(d.dbName)
	if err != nil {
		fmt.Println("Error dbName", d.dbName)
		os.Exit(1)
	}

	return db
}

func (d *LazyStrI64Dict) ToString() string {
	return fmt.Sprintf("<LazyStrI64Dict %v>", d.stri64Factory.ToString())
}
