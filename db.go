/*
	2017.07.23 
	a simple key-value embeddable database for golang practice
	fastball160@gmail.com
*/

package gokvdb

import (
	"fmt"
)

const (
	
	PAGE_SIZE int = 4096
	STORAGE_PAGER_META_OFFSET = 64

	DBTYPE_BTREE uint8 = 1
	//DBTYPE_HASH uint8 = 2
)

type IDBIndex interface {
	SaveAndGetMeta() []byte
	GetIsChanged() bool
	SetIsChanged(val bool)
	ToString() string
}

type Storage struct {
	stream IStream
	pager IPager
	rootPageId uint32
	dbItems map[string]*DBItem
}

type DBItem struct {
	metaPageId uint32
	name string	
	storage *Storage
	ctx *DBContext
}

type DBContext struct {	
	name string
	pageIdByMetaName map[string]uint32
	dbSets map[string]*DBSet
	pager IPager
}

type DBSet struct {
	name string
	dbType uint8
	metaPageId uint32
	obj interface{}
}

func (s *Storage) ToString() string {
	return fmt.Sprintf("<Storage rootPageId=%v path=%v>", s.rootPageId, s.stream.ToString())
}


func (ctx *DBItem) ToString() string {
	return fmt.Sprintf("<DBItem name=%v metaPageId=%v>", ctx.name, ctx.metaPageId)
}

func (ctx *DBContext) ToString() string {
	return fmt.Sprintf("<DBContext name=%v>", ctx.name)
}

func (s *DBSet) ToString() string {
	return fmt.Sprintf("<DBSet name=%v dbType=%v metaPageId=%v>", s.name, s.dbType, s.metaPageId)
}

func OpenStorage(path string) (*Storage, error) {

	stream, err := OpenFileStream(path)
	if err != nil {
		stream.Close()
		return nil, err
	}	

	var rootPageId uint32
	var pageSize uint32
	pageSize = 4096

	stream.Seek(0)
	headerData, err := stream.Read(512)
	var pagerMeta []byte

	if err == nil {
		rd := NewDataStreamFromBuffer(headerData)

		rootPageId = rd.ReadUInt32()
		rd.Seek(STORAGE_PAGER_META_OFFSET)
		pagerMeta = rd.Read(128)

		
		

	} else {
		rootPageId = 0
		pagerMeta = nil
	}
	//_CheckErr("Read...", err)

	//fmt.Println("PAGER META >>", pagerMeta)

	meta := ReadOrNewStreamPagerMeta(pageSize, pagerMeta)
	pager := NewStreamPager(stream, meta)

	//fmt.Printf("PAGER >> %v\n", pager.ToString())

	storage := new(Storage)
	storage.stream = stream
	storage.pager = pager
	storage.dbItems = make(map[string]*DBItem)

	if rootPageId == 0 {

		rootPageId = pager.CreatePageId()

	} else {
		rootData, err := pager.ReadPayloadData(rootPageId)
		_CheckErr("LOAD ROOT", err)
		rootR := NewDataStreamFromBuffer(rootData)

		dbCount := rootR.ReadUInt16()

		var i uint16
		var dbMetaPagId uint32
		var dbName string

		for i=0; i<dbCount; i++ {
			
			dbMetaPagId = rootR.ReadUInt32()
			dbName = rootR.ReadHStr()

			dbItem := storage._NewDBItem(dbName, dbMetaPagId)

			storage.dbItems[dbItem.name] = dbItem
			//fmt.Println("LOAD DBItem", dbItem.ToString())
		}
	}

	storage.rootPageId = rootPageId
	
	//fmt.Println(strings.Repeat("-", 30))

	//fmt.Println("OPEN", storage.ToString())

	return storage, nil
}

func (s *Storage) _NewDBItem(name string, metaPageId uint32) *DBItem {
	item := new(DBItem)
	item.storage = s
	item.name = name
	item.metaPageId = metaPageId

	return item
}

func (s *Storage) DB(name string) (*DBContext, error) {

	item, ok := s.dbItems[name]
	if !ok {
		metaPageId := s.pager.CreatePageId()
		item = s._NewDBItem(name, metaPageId)

		ctx, err := _OpenDBContext(name, s.pager, make([]byte, 256))
		_CheckErr("_OpenDBContext", err)
		item.ctx = ctx		

		s.dbItems[name] = item
	}

	if item.ctx == nil {
		dbMeta, err := s.pager.ReadPayloadData(item.metaPageId)
		_CheckErr("DB", err)
		ctx, err := _OpenDBContext(name, s.pager, dbMeta)
		_CheckErr("_OpenDBContext", err)
		item.ctx = ctx
	}

	return item.ctx, nil
}

func (s *Storage) Close() {

	if s.stream != nil {
		s.stream.Close()
		s.stream = nil
	}
}


func (s *Storage) Save() {	
	rootW := NewDataStream()
	rootW.WriteUInt16(uint16(len(s.dbItems)))

	for _, dbItem := range s.dbItems {
		//fmt.Println("SAVE DBContext", name)

		if dbItem.ctx != nil {
			dbCtxMeta := dbItem.ctx.Save()
			s.pager.WritePayloadData(dbItem.metaPageId, dbCtxMeta)
		}		

		rootW.WriteUInt32(dbItem.metaPageId)
		rootW.WriteHStr(dbItem.name)
	}

	s.pager.WritePayloadData(s.rootPageId, rootW.ToBytes())


	pageMeta := s.pager.Save()

//	metaWriteOffset := int64(0)

	hdrW := NewDataStream()

	hdrW.WriteUInt32(s.rootPageId)

	if pageMeta != nil {
		hdrW.Seek(STORAGE_PAGER_META_OFFSET)
		hdrW.Write(pageMeta)		
	}	

	s.stream.Seek(0)
	s.stream.Write(hdrW.ToBytes())

	s.stream.Sync()
}

func (ctx *DBContext) Save() []byte {

	rootW := NewDataStream()
	
	rootW.WriteUInt32(uint32(len(ctx.dbSets)))

	pager := ctx.pager

	for _, dset := range ctx.dbSets {
		//fmt.Println("SAVE DSET", name)

		switch dset.dbType {
		case DBTYPE_BTREE:
			db := dset.obj.(IDBIndex)
			if db.GetIsChanged() {
				//fmt.Println("SAVE BTreeBlobMap", dset, db.ToString())
				//tb := dset.obj.(IDBIndex)
				meta := db.SaveAndGetMeta()
				//fmt.Println("SAVE BTree META", meta)
				pager.WritePayloadData(dset.metaPageId, meta)
				db.SetIsChanged(false)	
			}
		}		

		rootW.WriteUInt8(dset.dbType)
		rootW.WriteUInt32(dset.metaPageId)
		rootW.WriteHStr(dset.name)
	}

	rootW.WriteUInt32(uint32(len(ctx.pageIdByMetaName)))

	for metaName, pgId := range ctx.pageIdByMetaName {
		rootW.WriteHStr(metaName)
		rootW.WriteUInt32(pgId)
	}

	return rootW.ToBytes()
}

func (ctx *DBContext) GetMeta(name string) ([]byte, bool) {

	pid, ok := ctx.pageIdByMetaName[name]
	if ok {
		data, err := ctx.pager.ReadPayloadData(pid)
		if err == nil {
			return data, true
		}
	}

	return nil, false
}

func (ctx *DBContext) SetMeta(name string, data []byte) {

	pid, ok := ctx.pageIdByMetaName[name]
	if !ok {
		pid = ctx.pager.CreatePageId()
		ctx.pageIdByMetaName[name] = pid
	}
	ctx.pager.WritePayloadData(pid, data)
}

func _OpenDBContext(name string, pager IPager, meta []byte) (*DBContext, error) {
	ctx := new(DBContext)
	ctx.name = name
	ctx.pager = pager
	ctx.dbSets = make(map[string]*DBSet)
	ctx.pageIdByMetaName = make(map[string]uint32)

	rootR := NewDataStreamFromBuffer(meta)

	itemCount := rootR.ReadUInt32()

	var i uint32
	var dsetType uint8
	var dsetMetaPagId uint32
	var dsetName string

	for i=0; i<itemCount; i++ {
		dsetType = rootR.ReadUInt8()
		dsetMetaPagId = rootR.ReadUInt32()
		dsetName = rootR.ReadHStr()

		dset := new(DBSet)
		dset.dbType = dsetType
		dset.metaPageId = dsetMetaPagId
		dset.name = dsetName

		ctx.dbSets[dset.name] = dset
		//fmt.Println("LOAD DSet", dset.ToString())
	}

	metaCount := rootR.ReadUInt32()

	for i=0 ; i<metaCount; i++ {
		metaName := rootR.ReadHStr()
		metaPid := rootR.ReadUInt32()

		ctx.pageIdByMetaName[metaName] = metaPid
	}

	
	//fmt.Println(strings.Repeat("-", 30))

	//fmt.Println("OPEN", ctx.ToString())

	return ctx, nil
}


type BTreeIndex struct {
	bt *BTreeBlobMap
	internalPager IPager
	isChanged bool
}


func (ix *BTreeIndex) ToString() string {
	return fmt.Sprintf("<BTreeTable %v>", ix.bt.ToString())
}

func (ix *BTreeIndex) Set(key int64, value []byte) {
	ix.bt.Set(key, value)
	ix.isChanged = true
}

func (ix *BTreeIndex) Get(key int64) ([]byte, bool){
	return ix.bt.Get(key)
}

func (ix *BTreeIndex) GetIsChanged() bool {
	return ix.isChanged
	//return bt.bt.isChanged
}

func (ix *BTreeIndex) SetIsChanged(val bool) {
	//bt.bt.isChanged = val
	ix.isChanged = val
}


func (ix *BTreeIndex) SaveAndGetMeta() []byte {

	meta2 := ix.bt.Save()
	meta1 := ix.internalPager.Save()

	w := NewDataStreamFromBuffer(make([]byte, 128))
	w.Write(meta1)
	w.Seek(64)
	w.Write(meta2)

	return w.ToBytes()
}

func _NewBTreeIndex(internalPager IPager, btMap *BTreeBlobMap) *BTreeIndex {
	bt := new(BTreeIndex)
	bt.bt = btMap
	bt.internalPager = internalPager

	return bt
}


func (s *DBContext) OpenBTree(name string) (*BTreeIndex, error) {

	internalPageSize := uint16(96)

	dset, ok := s.dbSets[name]
	if !ok {
		dset = new(DBSet)
		dset.dbType = DBTYPE_BTREE
		dset.name = name

		metaPageId := s.pager.CreatePageId()
		dset.metaPageId = metaPageId

		internalPager := NewInternalPager(s.pager, internalPageSize, nil)
		btMap := NewBTreeBlobMap(internalPager, nil)

		bt := _NewBTreeIndex(internalPager, btMap)

		dset.obj = bt

		s.dbSets[name] = dset
	}

	if dset.obj == nil {

		metaData, err := s.pager.ReadPayloadData(dset.metaPageId)

		if err != nil {
			return nil, err
		}

		//fmt.Println("LOAD BTREE META", metaData)

		rd := NewDataStreamFromBuffer(metaData)

		internalMeta := rd.Read(64)
		rd.Seek(64)
		btMeta := rd.Read(64)

		internalPager := NewInternalPager(s.pager, internalPageSize, internalMeta)
		btMap := NewBTreeBlobMap(internalPager, btMeta)

		bt := _NewBTreeIndex(internalPager, btMap)

		//fmt.Println(">> OPEN OpenBTree", bt.ToString())

		dset.obj = bt
	}

	return dset.obj.(*BTreeIndex), nil
}


