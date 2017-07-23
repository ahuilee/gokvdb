/*
	2017.07.23 
	a simple key-value embeddable database for golang practice
	fastball160@gmail.com
*/
package gokvdb

import (
	"fmt"
	"os"
	"path/filepath"
	"hash/fnv"
	"bytes"
	"strings"
	"encoding/binary"
	"encoding/gob"
)

const HEADER_SIZE int = 512
const PAGE_SIZE int = 8192
const PAGE_HEADER_SIZE int = 16

type DB struct {
	file *os.File
	path string
	header *Header

	byteOrder binary.ByteOrder
	keyRoot *KeyRoot
	valRoot *ValRoot

	keyPages map[uint32]*KeyPage
	valPages map[uint32]*ValPage
}

type Header struct {
	lastPageId uint32
	lastRowId uint64
	keyRootPageId uint32
	valRootPageId uint32
}

type PageHeader struct {
	nextPageId uint32
	dataLen uint16
}

type Page struct {
	pid uint32
	header PageHeader
	data []byte
}

type Item struct {
	rowId uint64
	key string
	//value []byte
	db *DB
}

type KeyPage struct {
	pid uint32
	rowIdByKey map[string]uint64
	changed bool
}

type ValPage struct {
	pid uint32
	valueByRowId map[uint64][]byte
	changed bool
}

type KeyRoot struct {
	pid uint32
	changed bool
	keyPageIdByHash map[uint32]uint32
}

type ValRoot struct {
	pid uint32
	changed bool
	valPageIdByBranch map[uint32]uint32
}

func (p *Page) ToString() string {
	return fmt.Sprintf("<Page id=%v %v>", p.pid, p.header.ToString())
}

func (h *PageHeader) ToString() string {
	return fmt.Sprintf("<PageHeader nextPageId=%v dataLen=%v>", h.nextPageId, h.dataLen)
}

func _HashKey(key string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32() & 0x1fff
}

func (db *DB) ToString() string {
	return fmt.Sprintf("<DB path=%v>", db.path)
}

func (h *Header) ToString() string {
	return fmt.Sprintf("<Header lastPageId=%v lastRowId=%v keyRootPageId=%v valRootPageId=%v>", h.lastPageId, h.lastRowId, h.keyRootPageId, h.valRootPageId)
}

func OpenHash(path string) *DB {
	db := new(DB)

	fullpath, _ := filepath.Abs(path)
	dirpath := filepath.Dir(fullpath)
	_, err := os.Stat(dirpath)
	if err != nil {
		if os.IsNotExist(err) {
			os.MkdirAll(dirpath, os.ModePerm)
		}
	}
	db.byteOrder = binary.LittleEndian
	db.path = fullpath

	db.file, err = os.OpenFile(fullpath, os.O_RDWR | os.O_CREATE, 0666)
	fmt.Println(db, err, fullpath)
	
	db.header = _ReadHeader(db.file, db.byteOrder)
	db.keyPages = make(map[uint32]*KeyPage)
	db.valPages = make(map[uint32]*ValPage)

	fmt.Println(strings.Repeat("-", 30))
	fmt.Println("OPEN DB")

	fmt.Println(db.header.ToString())

	return db
}

func (db *DB) Close() {
	if db.file != nil {
		db.file.Close()
		db.file = nil
	}
}

func (i *Item) Key() string {
	return i.key
}

func (i *Item) Value() []byte {
	value, ok := i.db._GetValueByRowId(i.rowId)
	if !ok {
		fmt.Println("_GetValueByRowId !ok")
		os.Exit(1)
	}

	return value
}

func (db *DB) Items() chan Item {	

	q := make(chan Item)

	go func(ch chan Item) {
		root := db._GetKeyRoot()

		for _, pid := range root.keyPageIdByHash {
			keyPage := db._GetKeyPageByPid(pid)
			//fmt.Println("Items", "hashKey", hashKey, pid)
			
			for key, rowId := range keyPage.rowIdByKey {
				
				//fmt.Println("DB Items", key, len(value))
				item := Item{rowId: rowId, key: key, db: db}
				ch <-item
			}
		}

		fmt.Println("Items END")

		close(ch)
	}	(q)

	return q
}

func (db *DB) _Set(key string, value []byte) {

	keyPage := db._GetKeyPage(key)

	rowId, ok := keyPage.rowIdByKey[key]
	if !ok {
		rowId = db._CreateRowId()
		keyPage.rowIdByKey[key] = rowId
		keyPage.changed = true
	}

	valPage := db._GetValuePageByRowId(rowId)
	valPage.valueByRowId[rowId] = value
	valPage.changed = true

}

func (db *DB) Set(key string, value []byte) {
	db._Set(key, value)
	db.Save()
}

func (db *DB) Get(key string) ([]byte, bool) {
	keyPage := db._GetKeyPage(key)

	//fmt.Println("key", key, "GET KEYPAGE", keyPage)
	
	rowId, ok := keyPage.rowIdByKey[key]
	if ok {
		valPage := db._GetValuePageByRowId(rowId)
		//fmt.Println("rowId", rowId, "GET VALPAGE", valPage)
		val, ok := valPage.valueByRowId[rowId]

		return val, ok
	}

	return nil, false
}

func (db *DB) Update(data map[string][]byte) {

	fmt.Println(db.ToString(), "UPDATE", len(data))

	for k, v := range data {
		db._Set(k, v)
	}

	db.Save()

}


func _PackPageHeader(pageHeader PageHeader, order binary.ByteOrder) []byte {
	
	buf := new(bytes.Buffer)

	binary.Write(buf, order, uint32(pageHeader.nextPageId))
	binary.Write(buf, order, uint16(pageHeader.dataLen))

	padding := make([]byte, PAGE_HEADER_SIZE - buf.Len())

	return append(buf.Bytes(), padding...)
}

func _PackHeader(hdr *Header, order binary.ByteOrder) []byte {
	
	buf := new(bytes.Buffer)
	binary.Write(buf, order, uint32(hdr.lastPageId))
	binary.Write(buf, order, uint64(hdr.lastRowId))
	binary.Write(buf, order, uint32(hdr.keyRootPageId))
	binary.Write(buf, order, uint32(hdr.valRootPageId))

	//fmt.Println("_PackHeader hdr.keyRootPageId", hdr.keyRootPageId, hdr.valRootPageId)

	return buf.Bytes()
}

func _ReadHeader(f *os.File, order binary.ByteOrder) *Header {

	hdrBytes := make([]byte, HEADER_SIZE)
	f.Seek(0, os.SEEK_SET)
	_, err := f.Read(hdrBytes)

	//fmt.Println("_ReadHeader", hdrBytes)

	var lastPageId uint32
	var lastRowId uint64
	var keyRootPageId uint32
	var valRootPageId uint32

	hdr := new(Header)	

	if err != nil {
		
		return hdr
	}

	buf := bytes.NewReader(hdrBytes)

	binary.Read(buf, order, &lastPageId)
	binary.Read(buf, order, &lastRowId)
	binary.Read(buf, order, &keyRootPageId)
	binary.Read(buf, order, &valRootPageId)

	hdr.lastPageId = lastPageId
	hdr.lastRowId = lastRowId
	hdr.keyRootPageId = keyRootPageId
	hdr.valRootPageId = valRootPageId
	

	return hdr
}


func (db *DB) Save() {
	//fmt.Println(strings.Repeat("-", 30))
	//fmt.Println("SAVE", db.ToString())

	var pageData []byte

	f := db.file

	if db.keyRoot != nil {
		if db.keyRoot.changed {
			db.keyRoot.changed = false
			pageData = _Pack2Bytes(db.keyRoot.keyPageIdByHash)
			//fmt.Println("SAVE KEY ROOT", db.keyRoot.pid)
			db._SavePayloadData(db.keyRoot.pid, pageData)
		}
	}

	if db.valRoot != nil {
		if db.valRoot.changed {
			db.valRoot.changed = false
			pageData = _Pack2Bytes(db.valRoot.valPageIdByBranch)
			//fmt.Println("SAVE VAL ROOT", db.keyRoot.pid)
			db._SavePayloadData(db.valRoot.pid, pageData)
		}
	}

	//fmt.Println("db.keyPages", len(db.keyPages))

	for pid, keyPage := range db.keyPages {
		if keyPage.changed {
			//fmt.Println(strings.Repeat("-", 30))
			//fmt.Println("** KEYPAGE SAVE **", pid)
			keyPage.changed = false
			pageData = _Pack2Bytes(keyPage.rowIdByKey)
			//fmt.Println(">> KEYPAGE SAVE", keyPage.rowIdByKey)
			db._SavePayloadData(pid, pageData)
		}
	}

	for pid, page := range db.valPages {
		if page.changed {
			//fmt.Println(strings.Repeat("-", 30))
			//fmt.Println("** KEYPAGE SAVE **", pid)
			page.changed = false
			pageData = _Pack2Bytes(page.valueByRowId)
			//fmt.Println(">> KEYPAGE SAVE", pageData)
			db._SavePayloadData(pid, pageData)
		}
	}


	headerData := _PackHeader(db.header, db.byteOrder)

	//fmt.Println(strings.Repeat("-", 30))
	//fmt.Println("SAVE header", headerData)

	f.Seek(0, os.SEEK_SET)
	f.Write(headerData)
}



func (db *DB) _CreatePageId() uint32 {
	pid := db.header.lastPageId + 1
	db.header.lastPageId = pid
	return pid
}

func (db *DB) _CreateRowId() uint64 {
	vid := db.header.lastRowId + 1
	db.header.lastRowId = vid
	return vid
}

func (db *DB) _NewKeyPage() *KeyPage {
	
	keyPage := new(KeyPage)
	keyPage.pid = 0
	keyPage.rowIdByKey = make(map[string]uint64)

	return keyPage
}

func (db *DB) _NewValPage() *ValPage {
	
	valPage := new(ValPage)
	valPage.pid = 0
	valPage.valueByRowId = make(map[uint64][]byte)

	return valPage
}


func (db *DB) _CreateKeyPage() *KeyPage {

	keyPage := db._NewKeyPage()
	keyPage.pid = db._CreatePageId()
	keyPage.changed = true

	return keyPage
}

func (db *DB) _CreateValPage() *ValPage {

	valPage := db._NewValPage()
	valPage.pid = db._CreatePageId()
	valPage.changed = true

	return valPage
}




func (db *DB) _LoadPageData(pid uint32, size int) (int, error, []byte) {

	f := db.file

	pageData := make([]byte, size)
	seek2 := int64(pid) * int64(PAGE_SIZE)
	f.Seek(seek2, os.SEEK_SET)
	rtn, err := f.Read(pageData)

	return rtn, err, pageData
}

func _LoadPageHeder(data []byte, order binary.ByteOrder) PageHeader {

	var nextPageId uint32
	var dataLen uint16

	buf := bytes.NewReader(data)

	binary.Read(buf, order, &nextPageId)
	binary.Read(buf, order, &dataLen)

	return PageHeader{nextPageId: nextPageId, dataLen: dataLen}
}

func (db *DB) _LoadPage(pid uint32) *Page {

	if pid < 1 {
		fmt.Println("LOAD PAGE ERROR pid", pid)
		os.Exit(1)
	}

	rtn, err, pageData := db._LoadPageData(pid, PAGE_SIZE)

	if err == nil {
		if rtn == PAGE_SIZE {

			pageHeader := _LoadPageHeder(pageData, db.byteOrder)

			page := new(Page)
			page.pid = pid
			page.header = pageHeader
			page.data = pageData[PAGE_HEADER_SIZE:(PAGE_HEADER_SIZE+int(pageHeader.dataLen))]		
			return page
		}
	}

	return nil
}

func (db *DB) _LoadPayloadPage(pid uint32) []byte {

		//fmt.Println("_LoadPayloadPage", pid)
		var allBytes []byte
		nextPageId := pid
		loopCount := 0

		for {
			if nextPageId == 0 {
				break
			}

			loopCount += 1
			//fmt.Println("_LoadPayloadPage loopCount", loopCount, "pid", pid)
			page := db._LoadPage(nextPageId)
			if page == nil {
				break
			}
			//fmt.Println("_LoadPayloadPage", "page=", page.ToString())
			allBytes = append(allBytes, page.data...)
			nextPageId = page.header.nextPageId
		}
		return allBytes
}

func (db *DB) _GetKeyRoot() *KeyRoot {

	if db.keyRoot == nil {
		fmt.Println("db.keyPage == nil", db.header.keyRootPageId)
		keyRoot := new(KeyRoot)
		keyRoot.keyPageIdByHash = make(map[uint32]uint32)

		if db.header.keyRootPageId == 0 {
			pid := db._CreatePageId()
			db.header.keyRootPageId = pid	

		} else {

			dataBytes := db._LoadPayloadPage(db.header.keyRootPageId)

			fmt.Println("_GetKeyRoot >> _LoadPayloadPage", len(dataBytes))

			dict := _UnpackBytes(dataBytes, keyRoot.keyPageIdByHash)

			keyRoot.keyPageIdByHash = dict.(map[uint32]uint32)

			fmt.Println(">> GetKeyRoot SUCCESS:")
		}

		keyRoot.pid = db.header.keyRootPageId

		db.keyRoot = keyRoot
	}

	return db.keyRoot
}

func (db *DB) _GetValRoot() *ValRoot {

	if db.valRoot == nil {
		fmt.Println("db.valRoot == nil", db.header.valRootPageId)
		valRoot := new(ValRoot)
		valRoot.valPageIdByBranch = make(map[uint32]uint32)
		

		if db.header.valRootPageId == 0 {
			pid := db._CreatePageId()
			db.header.valRootPageId = pid	

		} else {

			dataBytes := db._LoadPayloadPage(db.header.valRootPageId)

			fmt.Println("_GetValRoot >> _LoadPayloadPage", len(dataBytes))

			dict := _UnpackBytes(dataBytes, valRoot.valPageIdByBranch)

			valRoot.valPageIdByBranch = dict.(map[uint32]uint32)

			fmt.Println(">> GetKeyRoot SUCCESS:")
		}

		valRoot.pid = db.header.valRootPageId

		db.valRoot = valRoot
	}

	return db.valRoot
}

func (db *DB) _GetValueByRowId(rowId uint64) ([]byte, bool) {

	valPage := db._GetValuePageByRowId(rowId)

	val, ok := valPage.valueByRowId[rowId]

	return val, ok
}

func (db *DB) _GetValuePageByRowId(rowId uint64) *ValPage {

	branchKey := uint32(rowId / 128)
	root := db._GetValRoot()

	pid, ok := root.valPageIdByBranch[branchKey]

	if !ok {
		//fmt.Println("!ok", pid, ok)
		valPage := db._CreateValPage()

		root.valPageIdByBranch[branchKey] = valPage.pid
		root.changed = true
		pid = valPage.pid

		db.valPages[pid] = valPage
		return valPage
	}

	return db._GetValPageByPid(pid)
}

func (db *DB) _GetValPageByPid(pid uint32) *ValPage {
	var valPage *ValPage

	valPage, ok := db.valPages[pid]
	if !ok {
		
		dataBytes := db._LoadPayloadPage(pid)
		
		valPage = db._NewValPage()
		valPage.pid = pid

		fmt.Println("LOAD valueByRowId dataBytes", len(dataBytes))

		valueByRowId := _UnpackBytes(dataBytes, valPage.valueByRowId).(map[uint64][]byte)
		//fmt.Println("LOAD valueByRowId", valueByRowId)
		valPage.valueByRowId = valueByRowId
		db.valPages[pid] = valPage
	}

	return valPage

}

func (db *DB) _GetKeyPage(key string) *KeyPage {

	hashKey := _HashKey(key)
	root := db._GetKeyRoot()

	pid, ok := root.keyPageIdByHash[hashKey]

	if !ok {
		//fmt.Println("!ok", pid, ok)
		keyPage := db._CreateKeyPage()

		root.keyPageIdByHash[hashKey] = keyPage.pid
		root.changed = true
		pid = keyPage.pid

		db.keyPages[pid] = keyPage
		return keyPage
	}

	return db._GetKeyPageByPid(pid)
}

func (db *DB) _GetKeyPageByPid(pid uint32) *KeyPage {
	var keyPage *KeyPage

	keyPage, ok := db.keyPages[pid]
	if !ok {
		//fmt.Println("** LOAD KEYPAGE **")
		dataBytes := db._LoadPayloadPage(pid)
		//fmt.Println("LOAD KEYPAGE SUCCESS:", len(dataBytes))
		keyPage = db._NewKeyPage()
		keyPage.pid = pid

		rowIdByKey := _UnpackBytes(dataBytes, keyPage.rowIdByKey).(map[string]uint64)
		keyPage.rowIdByKey = rowIdByKey
		db.keyPages[pid] = keyPage
	}

	return keyPage
}

func (db *DB) _SavePayloadData(pid uint32, data []byte) {
	//fmt.Println("_SavePayloadData", "pid=", pid)

	if pid < 1 {
		fmt.Println("_SavePayloadData ERROR pid=", pid)
		os.Exit(1)
	}

	var curPageId uint32
	var nextPageId uint32
	var seek2 int64
	var iStart int
	var iEnd int

	nextPageId = pid

	pageContentSize := PAGE_SIZE - PAGE_HEADER_SIZE
	iStart = 0
	f := db.file

	loopCount := 0

	for {
		if iStart >= len(data) {
			break
		}

		loopCount += 1

		curPageId = nextPageId

		var pageHeader PageHeader

		_, err, pageHeaderData := db._LoadPageData(curPageId, PAGE_HEADER_SIZE)
		if err == nil {
			pageHeader = _LoadPageHeder(pageHeaderData, db.byteOrder)
		} else {
			pageHeader = PageHeader{nextPageId: 0, dataLen: 0}
		}

		iEnd = iStart + pageContentSize

		if iEnd >= len(data) {
			iEnd = len(data)
		} else {
			if pageHeader.nextPageId == 0 {
				pageHeader.nextPageId = db._CreatePageId()
			}
		}

		//fmt.Println("iStart", iStart, "iEnd", iEnd)

		pageContentData := data[iStart:iEnd]

		//fmt.Println("pageContentData", len(pageContentData))

		pageHeader.dataLen = uint16(len(pageContentData))
		pageHeaderData = _PackPageHeader(pageHeader, db.byteOrder)

		//fmt.Println("pageHeaderData", len(pageHeaderData))

		if len(pageHeaderData) != PAGE_HEADER_SIZE {
			fmt.Println(fmt.Sprintf("len(pageHeaderData) != %v", PAGE_HEADER_SIZE))
			os.Exit(1)
		}

		var pageData []byte
		pageData = append(pageData, pageHeaderData...)
		pageData = append(pageData, pageContentData...)

		pagePaddingSize := PAGE_SIZE - len(pageData)
		if pagePaddingSize < 0 {
			fmt.Println(fmt.Sprintln("pagesize over %v", PAGE_SIZE))
			os.Exit(1)
		}

		if pagePaddingSize > 0 {
			pageData = append(pageData, make([]byte, pagePaddingSize)...)			
		}

		//fmt.Println("SAVE pageData", "pid=", curPageId, "len", len(pageData))

		seek2 = int64(curPageId) * int64(PAGE_SIZE)
		f.Seek(seek2, os.SEEK_SET)
		f.Write(pageData)

		nextPageId = pageHeader.nextPageId
		iStart = iEnd
	}
}

func _CheckErr(message string, err error) {
	if err != nil {
		fmt.Println(message, "ERROR", err)
		os.Exit(1)
	}
}

func _UnpackBytes(data []byte, unpackType interface{}) interface{} {

	var err error

	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)	

	switch unpackType.(type) {
		case map[uint32]uint32:
			var dict map[uint32]uint32
			err = dec.Decode(&dict)
			_CheckErr("_UnpackBytes map[uint32]uint32", err)
			return dict
		case map[uint32]string:
			var dict map[uint32]string
			err = dec.Decode(&dict)
			_CheckErr("_UnpackBytes map[uint32]string", err)
			return dict
		case map[string][]byte:
			var obj map[string][]byte
			err = dec.Decode(&obj)
			_CheckErr("_UnpackBytes map[string][]byte", err)
			return obj

		case map[string]uint64:
			var obj map[string]uint64
			err = dec.Decode(&obj)
			_CheckErr("_UnpackBytes map[string]uint64", err)
			return obj
		case map[uint64][]uint8:
			var obj map[uint64][]uint8
			err = dec.Decode(&obj)
			_CheckErr("_UnpackBytes map[uint64][]uint8", err)
			return obj
	}

	return nil
}

func _Pack2Bytes(obj interface{}) []byte {

	var buf bytes.Buffer

	enc := gob.NewEncoder(&buf)
	err := enc.Encode(obj)
	_CheckErr("_Pack2Bytes", err)

	return buf.Bytes()
}
