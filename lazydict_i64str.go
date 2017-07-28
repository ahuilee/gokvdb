package gokvdb


import (
	"fmt"
	"os"
	//"hash/fnv"
)

type LazyI64StrDict struct {
	dbName string
	dictName string
	storage *Storage
	
	getContextByBranchKey map[int64]*LazyI64StrContext

	//bt *BTreeIndex

	internalPager IPager
	bt *BTreeBlobMap
	isChanged bool
}

type LazyI64StrContext struct {
	btKey int64
	getValueByKey map[int64]string
	isChanged bool
	dict *LazyI64StrDict
}

func NewI64StrDict(s *Storage, dbName string, dictName string) *LazyI64StrDict {

	dict := new(LazyI64StrDict)
	dict.dbName = dbName
	dict.dictName = dictName
	dict.storage = s
	dict.getContextByBranchKey = make(map[int64]*LazyI64StrContext)

	db, err := s.DB(dbName)
	if err != nil {
		fmt.Println("error dbName", dbName)
		os.Exit(1)
	}

	metaData, ok := db.GetMeta(dictName)

	//fmt.Println("NewI64StrDict metaData", ok, metaData)

	var internalPagerMeta []byte
	var btMeta []byte

	if ok {

		rd := NewDataStreamFromBuffer(metaData)
		internalPagerMeta = rd.ReadChunk()
		btMeta = rd.ReadChunk()

	} 

	internalPageSize := uint16(128)

	internalPager := NewInternalPager(s.pager, internalPageSize, internalPagerMeta)
	//fmt.Println("NewI64StrDict internalPager", internalPager.ToString())

	btMap := NewBTreeBlobMap(internalPager, btMeta)

	
	//fmt.Println("NewI64StrDict btMap", btMap.ToString())
	

	dict.internalPager = internalPager
	dict.bt = btMap

	return dict
}

func (d *LazyI64StrDict) ToString() string {
	return fmt.Sprintf("<LazyI64StrDict db=%v name=%v>", d.dbName, d.dictName)
}

func (c *LazyI64StrContext) ToString() string {
	return fmt.Sprintf("<LazyI64StrContext btKey=%v>", c.btKey)
}

func (d *LazyI64StrDict) _GetBt() *BTreeBlobMap {
	/*
	if d.bt == nil {
		db, _ := d.storage.DB(d.dbName)
		bt, _ := db.OpenBTree(d.dictName)
		d.bt = bt
	}*/
	return d.bt
}

func (d *LazyI64StrDict) _ReadContext(branchKey int64, data []byte) *LazyI64StrContext {

	ctx := d._NewContext(branchKey)
			
	rd := NewDataStreamFromBuffer(data)
	rowsCount := int(rd.ReadUInt24())
	for i:=0; i<rowsCount; i++ {
		key := int64(rd.ReadUInt64())
		valChunk := rd.ReadChunk()
		ctx.getValueByKey[key] = string(valChunk)
	}
	return ctx
}

func (d *LazyI64StrDict) _GetContext(branchKey int64) *LazyI64StrContext {

	ctx, ok := d.getContextByBranchKey[branchKey]
	if !ok {
		bt := d._GetBt()
		data, ok := bt.Get(branchKey)
		//fmt.Println("_GetContext branchKey=", branchKey, data)
		if ok {

			ctx = d._ReadContext(branchKey, data)
			

			d.getContextByBranchKey[branchKey] = ctx
		}
	}

	return ctx

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

func (d *LazyI64StrDict) Items() chan LazyI64StrDictItem {
	q := make(chan LazyI64StrDictItem)
	go func(ch chan LazyI64StrDictItem) {

		bt := d.bt

		for branchItem := range bt.Items() {
			//fmt.Println("LazyI64StrDict Items bt", branchItem)
			branchKey := branchItem.Key()

			ctx, ok := d.getContextByBranchKey[branchKey]
			if !ok {
				ctx = d._ReadContext(branchKey, branchItem.Value())
			}

			for k, v := range ctx.getValueByKey {

				ch <- LazyI64StrDictItem{key: k, value: v}
			}
		}

		close(ch)
	}(q)

	return q
}

func (d *LazyI64StrDict) Set(key int64, value string) {
	//bt := d._GetBt()
	branchKey := d._GetBranchKey(key)
	ctx := d._GetContext(branchKey)
	if ctx == nil {
		ctx = d._NewContext(branchKey)
		ctx.isChanged = true
		d.getContextByBranchKey[branchKey] = ctx
	}

	//fmt.Println(d.ToString(), "SET", key, value, ctx.ToString())

	ctx.getValueByKey[key] = value
	ctx.isChanged = true
}

func (d *LazyI64StrDict) Get(key int64) (string, bool) {
	//bt := d._GetBt()
	branchKey := d._GetBranchKey(key)
	ctx := d._GetContext(branchKey)
	if ctx != nil {
		val, ok := ctx.getValueByKey[key]
		if ok {
			return val, true
		}
	}

	return "", false
}

func (d *LazyI64StrDict) Save() {
	//fmt.Println("Save", d.ToString())

	bt := d._GetBt()

	//isChanged := false

	for btKey, ctx := range d.getContextByBranchKey {
		if ctx.isChanged {

			w := NewDataStream()
			w.WriteUInt24(uint32(len(ctx.getValueByKey)))

			for k, v := range ctx.getValueByKey {
				w.WriteUInt64(uint64(k))
				w.WriteChunk([]byte(v))

				//fmt.Println("SAVE CONTEXT", k, v)
			}

			ctxData := w.ToBytes()

			bt.Set(btKey, ctxData)

			ctx.isChanged = false
			//isChanged = true
			fmt.Println("Save btKey=", btKey, "bytes", len(ctxData))
		}
	}


	db, err := d.storage.DB(d.dbName)
	if err != nil {
		fmt.Println("error dbName", d.dbName)
		os.Exit(1)
	}

	btMeta := bt.Save()
	internalPagerMeta := d.internalPager.Save()

	metaW := NewDataStream()
	metaW.WriteChunk(internalPagerMeta)
	metaW.WriteChunk(btMeta)

	metaBytes := metaW.ToBytes()

	db.SetMeta(d.dictName, metaBytes)

	//fmt.Println("SAVE", d.ToString(), metaBytes)

	//fmt.Println("SAVE internalPager", d.internalPager.ToString())
	//fmt.Println("SAVE btMap", d.bt.ToString())

	d.storage.Save()
}

func (d *LazyI64StrDict) _NewContext(btKey int64) *LazyI64StrContext {
	ctx := new(LazyI64StrContext)
	ctx.btKey = btKey
	ctx.getValueByKey = make(map[int64]string)
	ctx.isChanged = false

	return ctx
}

