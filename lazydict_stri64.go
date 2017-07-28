package gokvdb


import (
	"os"
	"fmt"
	"hash/fnv"
)

const (
	LAZYSTRI64_DATA = 1
	LAZYSTRI64_BRANCH = 2
)

type LazyStrI64Dict struct {
	dbName string
	dictName string

	lastContextId uint32
	rootContextId uint32
	pageIdByContextId map[uint32]uint32
	contextById map[uint32]*LazyStrI64Context
	storage *Storage
	//bt *BTreeIndex
	splitCount int
}

type LazyStrI64Context struct {
	id uint32
	pid uint32
	ctxType byte
	depth byte
	valueByKey map[string]int64
	childContextIdByBranchKey map[int32]uint32

	isChanged bool
	dict *LazyStrI64Dict
}

func NewStrI64Dict(s *Storage, dbName string, dictName string) *LazyStrI64Dict {

	dict := new(LazyStrI64Dict)
	dict.storage = s
	dict.dbName = dbName
	dict.dictName = dictName
	dict.contextById = make(map[uint32]*LazyStrI64Context)
	dict.pageIdByContextId = make(map[uint32]uint32)
	dict.splitCount = 0
	
	var lastContextId uint32
	var rootContextId uint32

	db, err := s.DB(dbName)
	if err != nil {
		fmt.Println("error dbName", dbName)
		os.Exit(1)
	}

	metaData, ok := db.GetMeta(dictName)

	if ok {
		rd := NewDataStreamFromBuffer(metaData)

		lastContextId = rd.ReadUInt32()
		rootContextId = rd.ReadUInt32()

		rowsCont := int(rd.ReadUInt32())

		for i:=0; i<rowsCont; i++ {
			ctxId := rd.ReadUInt32()
			pgId := rd.ReadUInt32()
			dict.pageIdByContextId[ctxId] = pgId
	
			fmt.Println("NewStrI64Dict LOAD item", "ctxId", ctxId, "pid", pgId)

		}

	}

	dict.lastContextId = lastContextId
	dict.rootContextId = rootContextId

	fmt.Println("NewStrI64Dict", dict.ToString())

	return dict
}

func (d *LazyStrI64Dict) ToString() string {
	return fmt.Sprintf("<LazyStrI64Dict lastContextId=%v rootContextId=%v>", d.lastContextId, d.rootContextId)
}

type LazyStrI64Item struct {
	key string
	value int64
}

func (i *LazyStrI64Item) Key() string {
	return i.key
}

func (i *LazyStrI64Item) Value() int64 {
	return i.value
}

func (d *LazyStrI64Dict) Items() chan LazyStrI64Item {

	q := make(chan LazyStrI64Item)

	go func(ch chan LazyStrI64Item) {

		root := d._GetRoot()
		
		root.TakeItems(ch)

		close(ch)
	} (q)

	return q
}

func (d *LazyStrI64Dict) Set(key string, value int64) {
	root := d._GetRoot()
	root.Set(key, value)
}

func (d *LazyStrI64Dict) Get(key string) (int64, bool) {
	root := d._GetRoot()
	val, ok := root.Get(key)
	return val, ok
}


func (d *LazyStrI64Dict) _GetDB() *DBContext {

	db, err := d.storage.DB(d.dbName)
	if err != nil {
		fmt.Println("Error dbName", d.dbName)
		os.Exit(1)
	}

	return db
}

func (d *LazyStrI64Dict) Save() {



	for ctxId, ctx := range d.contextById {
		if ctx.isChanged {
			//
			ctx.isChanged = false

			w := NewDataStream()
			w.WriteUInt8(ctx.ctxType)
			w.WriteUInt8(ctx.depth)

			switch ctx.ctxType {
			case LAZYSTRI64_DATA:
				w.WriteUInt16(uint16(len(ctx.valueByKey)))

				for k, v := range ctx.valueByKey {
					w.WriteHStr(k)
					w.WriteUInt64(uint64(v))
				}

			case LAZYSTRI64_BRANCH:
				w.WriteUInt16(uint16(len(ctx.childContextIdByBranchKey)))

				for k, v := range ctx.childContextIdByBranchKey {
					w.WriteUInt32(uint32(k))
					w.WriteUInt32(v)
				}
			}

			ctxData := w.ToBytes()

			//bt.Set(int64(ctxId), ctxData)
			d.storage.pager.WritePayloadData(ctx.pid, ctxData)

			fmt.Println("LazyStrI64Dict SAVE ctxId=", ctxId, "bytes", len(ctxData), "rows", len( ctx.valueByKey))
		}
	}

	db := d._GetDB()

	metaW := NewDataStream()

	metaW.WriteUInt32(d.lastContextId) 
	metaW.WriteUInt32(d.rootContextId) 

	metaW.WriteUInt32(uint32(len(d.pageIdByContextId)))

	for ctxId, pgId := range d.pageIdByContextId {
		metaW.WriteUInt32(ctxId)
		metaW.WriteUInt32(pgId)
	}

	d.splitCount = 0

	db.SetMeta(d.dictName, metaW.ToBytes())

	d.storage.Save()
}



func (d *LazyStrI64Dict) _GetRoot() *LazyStrI64Context {
	if d.rootContextId == 0 {
		root := d._CreateContext(LAZYSTRI64_DATA, 0)
		d.rootContextId = root.id
		return root
	}

	return d._GetContextById(d.rootContextId)
}

func (d *LazyStrI64Dict) _GetContextById(id uint32) *LazyStrI64Context {
	ctx, ok := d.contextById[id]

	if !ok {
		pid, ok := d.pageIdByContextId[id]
		if ok {
			//bt := d._GetBt()
			//data, ok := bt.Get(int64(id))
			data, err := d.storage.pager.ReadPayloadData(pid)
			if err == nil {
				rd := NewDataStreamFromBuffer(data)

				ctxType := rd.ReadUInt8()
				depth := rd.ReadUInt8()

				ctx = d._NewContext(id, pid, ctxType, depth)	

				var rowsCount int
				rowsCount = int(rd.ReadUInt16())

				fmt.Println("_GetContext", ctx.ToString(), "rowsCount", rowsCount)

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

				d.contextById[id] = ctx
			}
		}
	}

	return ctx
}

func (d *LazyStrI64Dict) _NewContext(id uint32, pid uint32, ctxType byte, depth byte) *LazyStrI64Context {

	ctx := new(LazyStrI64Context)
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

func (d *LazyStrI64Dict) _CreateContext(ctxType byte, depth byte) *LazyStrI64Context {
	id := d.lastContextId + 1
	d.lastContextId = id

	pid := d.storage.pager.CreatePageId()

	ctx := d._NewContext(id, pid, ctxType, depth)	
	ctx.isChanged = false
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

func (c *LazyStrI64Context) GetChildContext(key string) *LazyStrI64Context {

	hashKey := _HashString(key)
	branchSize := _GetBranchSize(c.depth)
	branchKey := (hashKey / branchSize) * branchSize

	return c.GetChildContextByBranchKey(branchKey)
}

func (c *LazyStrI64Context) GetChildContextByBranchKey(branchKey int32) *LazyStrI64Context {

	childCtxId, ok := c.childContextIdByBranchKey[branchKey]
	if ok {
		return c.dict._GetContextById(childCtxId)
	}

	return nil
}

func (c *LazyStrI64Context) GetOrCreateChildContext(key string) *LazyStrI64Context {
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

func (c *LazyStrI64Context) ToString() string {
	return fmt.Sprintf("<LazyStrI64Context id=%v ctxType=%v depth=%v>", c.id, c.ctxType, c.depth)
}


func (c *LazyStrI64Context) TakeItems(q chan LazyStrI64Item) {

	if c.ctxType == LAZYSTRI64_BRANCH {

		for _, ctxId := range c.childContextIdByBranchKey {

			childChildCtx := c.dict._GetContextById(ctxId)
			childChildCtx.TakeItems(q)

		}

		return
	}

	for k, v := range c.valueByKey {
		q <- LazyStrI64Item{key: k, value: v}
	}

}

func (c *LazyStrI64Context) Get(key string) (int64, bool) {
	//fmt.Println(c.ToString(), "Get", key)
	if c.ctxType == LAZYSTRI64_BRANCH {

		branchCtx := c.GetChildContext(key)

		return branchCtx.Get(key)
	}

	val, ok := c.valueByKey[key]

	return val, ok
}

func (c *LazyStrI64Context) Set(key string, value int64) {

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

	if c.dict.splitCount < 16 {
		if c.depth < 2 && len(c.valueByKey) > 8192 {
			c.ctxType = LAZYSTRI64_BRANCH

			for k, v := range c.valueByKey {
				fmt.Println("[SPLIT]", c.ToString(), k, v)
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
