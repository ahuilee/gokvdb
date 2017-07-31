package gokvdb


import (
	"fmt"
	"os"
	//"hash/fnv"
)

type LazyI64BlobDict struct {
	dbName string
	dictName string
	storage *Storage

	internalPager IPager
	bt *BTreeBlobMap
	isChanged bool
}

func NewI64BlobDict(s *Storage, dbName string, dictName string) *LazyI64BlobDict {

	dict := new(LazyI64BlobDict)
	dict.dbName = dbName
	dict.dictName = dictName
	dict.storage = s

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

	bt := NewBTreeBlobMap(internalPager, btMeta)
	

	dict.internalPager = internalPager
	dict.bt = bt

	return dict
}

func (d *LazyI64BlobDict) ToString() string {
	return fmt.Sprintf("<LazyI64BlobDict db=%v name=%v>", d.dbName, d.dictName)
}


type LazyI64BlobDictItem struct {
	key int64
	item BTreeBlobMapItem
}

func (i *LazyI64BlobDictItem) Key() int64 {
	return i.key
}

func (i *LazyI64BlobDictItem) Value() []byte {
	return i.item.Value()
}

func (d *LazyI64BlobDict) Items() chan LazyI64BlobDictItem {
	q := make(chan LazyI64BlobDictItem)

	go func(ch chan LazyI64BlobDictItem) {

		bt := d.bt

		for btItem := range bt.Items() {
			fmt.Println("LazyI64BlobDict Items bt", btItem)

			item := LazyI64BlobDictItem{key: btItem.Key(), item: btItem}
			ch <- item
			
		}

		close(ch)
	}(q)

	return q
}

func (d *LazyI64BlobDict) Set(key int64, value []byte) {
	//bt := d._GetBt()

	d.bt.Set(key, value)
}

func (d *LazyI64BlobDict) Get(key int64) ([]byte, bool) {
	//bt := d._GetBt()
	value, ok := d.bt.Get(key)

	return value, ok
}

func (d *LazyI64BlobDict) Save(commit bool) {
	//fmt.Println("Save", d.ToString())

	bt := d.bt

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

	if commit {
		d.storage.Save()
	}
}

