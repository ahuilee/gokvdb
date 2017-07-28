package gokvdb


import (
	"os"
	"fmt"
)


type LazyStrBlobDict struct {
	dbName string
	dictName string

	lastId int64
	idByKeyDict *LazyStrI64Dict
	internalPager IPager
	bt *BTreeBlobMap
	storage *Storage
}

func NewStrBlobDict(s *Storage, dbName string, dictName string) *LazyStrBlobDict {

	dict := new(LazyStrBlobDict)
	dict.storage = s
	dict.dbName = dbName
	dict.dictName = dictName
	dict.idByKeyDict = NewStrI64Dict(s, dbName, fmt.Sprintf("%s_idByKey", dictName))
	

	db, err := s.DB(dbName)
	if err != nil {
		fmt.Println("error dbName", dbName)
		os.Exit(1)
	}

	var lastId int64
	var internalPagerMeta []byte
	var btMeta []byte

	metaData, ok := db.GetMeta(dictName)

	if ok {
		rd := NewDataStreamFromBuffer(metaData)

		lastId = int64(rd.ReadUInt64())

		internalPagerMeta = rd.ReadChunk()
		btMeta = rd.ReadChunk()

	}

	internalPageSize := uint16(128)
	internalPager := NewInternalPager(s.pager, internalPageSize, internalPagerMeta)
	bt := NewBTreeBlobMap(internalPager, btMeta)

	dict.lastId = lastId
	dict.internalPager = internalPager
	dict.bt = bt

	fmt.Println("NewStrBlobDict", dict.ToString())

	return dict
}

func (d *LazyStrBlobDict) ToString() string {
	return fmt.Sprintf("<LazyStrBlobDict lastId=%v>", d.lastId)
}

type LazyStrBlobItem struct {
	id int64
	key string
	dict *LazyStrBlobDict
}

func (i *LazyStrBlobItem) Key() string {
	return i.key
}

func (i *LazyStrBlobItem) Value() []byte {
	data, ok := i.dict.bt.Get(i.id)
	if !ok {
		fmt.Println("Error no key", i.key)
	}
	return data
}

func (d *LazyStrBlobDict) Items() chan LazyStrBlobItem {

	q := make(chan LazyStrBlobItem)

	go func(ch chan LazyStrBlobItem) {

		for item := range d.idByKeyDict.Items() {
			key := item.Key()
			id := item.Value()

			ch <- LazyStrBlobItem{id: id, key: key, dict: d}
		}

		close(ch)
	} (q)

	return q
}

func (d *LazyStrBlobDict) _CreateId() int64 {
	id := d.lastId + 1
	d.lastId = id
	return id
}

func (d *LazyStrBlobDict) Set(key string, value []byte) {

	id, ok :=	d.idByKeyDict.Get(key)
	if !ok {
		id = d._CreateId()
		d.idByKeyDict.Set(key, id)
	}

	d.bt.Set(id, value)
}

func (d *LazyStrBlobDict) Get(key string) ([]byte, bool) {
	id, ok :=	d.idByKeyDict.Get(key)
	if ok {
		data, ok := d.bt.Get(id)
		return data, ok
	}
	return nil, false
}


func (d *LazyStrBlobDict) _GetDB() *DBContext {

	db, err := d.storage.DB(d.dbName)
	if err != nil {
		fmt.Println("Error dbName", d.dbName)
		os.Exit(1)
	}

	return db
}

func (d *LazyStrBlobDict) Save() {

	db := d._GetDB()

	btMeta := d.bt.Save()
	internalPagerMeta := d.internalPager.Save()

	metaW := NewDataStream()

	metaW.WriteUInt64(uint64(d.lastId)) 

	metaW.WriteChunk(internalPagerMeta)
	metaW.WriteChunk(btMeta)

	db.SetMeta(d.dictName, metaW.ToBytes())

	d.idByKeyDict.Save()
	d.storage.Save()
}
