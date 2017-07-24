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
	"sync"
	//"strings"
	//"sort"
	"encoding/binary"
)

const HEADER_SIZE int = 512
const PAGE_SIZE int = 4096
const PAGE_HEADER_SIZE int = 16
const PAGE_CONENT_SIZE = PAGE_SIZE - PAGE_HEADER_SIZE

const PGTYPE_KEY_PAGE uint8 = 1
const PGTYPE_VAL_PAGE uint8 = 2
const PGTYPE_SPACELIST uint8 = 3
const PGTYPE_HASH_NODE uint8 = 11
const PGTYPE_NON_CLUSTERED_NODE uint8 = 12



type DB struct {
	file *os.File
	path string
	header *Header

	byteOrder binary.ByteOrder
	//keyRoot *KeyRoot
	spaceList *SpaceList

	hashNodes map[uint32]*HashNode
	nonClusteredNodes map[uint32]*NonClusteredNode
	keyPages map[uint32]*KeyPage
	valPages map[uint32]*ValPage

	lock sync.Mutex
	
}

type Header struct {
	lastPageId uint32
	lastKeyId uint64
	lastBranchId uint64
	keyRootPageId uint32
	branchRootPageId uint32
	spaceListPageId uint32
}

func (h *Header) ToString() string {
	return fmt.Sprintf("<Header lastPageId=%v lastKeyId=%v lastBranchId=%v keyRootPageId=%v branchRootPageId=%v spaceListPageId=%v>", h.lastPageId, h.lastKeyId, h.lastBranchId, h.keyRootPageId, h.branchRootPageId, h.spaceListPageId)
}

type PageHeader struct {
	pageId uint32
	payloadPageId uint32
	dataLen uint16
}

type Page struct {
	pid uint32
	header PageHeader
	data []byte
}

type Item struct {
	key string
	info KeyInfo
	db *DB
}

type PgMeta struct {
	pid uint32
	pgType uint8
}

type PgMetaError struct {
	message string
}

func (e PgMetaError) Error() string {
	return fmt.Sprintf("PgMetaError: %v", e.message)
}

func (p PgMeta) Valid(pid uint32, pgType uint8) error {
	if pid != p.pid {
		return PgMetaError{message: fmt.Sprintf("pgType=%v pid error %v != %v", pgType, pid, p.pid)}
	}

	if pgType != p.pgType {
		return PgMetaError{message: fmt.Sprintf("pgType error %v != %v pid=%v", pgType, p.pgType, pid)}
	}

	return nil
}

type KeyInfo struct {
	keyId uint64	
	isDeleted bool	
	branchId uint64	
}

type KeyPage struct {
	pid uint32
	infoByKey map[string]KeyInfo
	changed bool
}

func (p *KeyPage) ToString() string {
	return fmt.Sprintf("<KeyPage pid=%v>", p.pid)
}

type ValPage struct {
	pid uint32
	valueByKeyId map[uint64][]byte
	changed bool
}

func (p *Page) ToString() string {
	return fmt.Sprintf("<Page id=%v %v>", p.pid, p.header.ToString())
}

func (h *PageHeader) ToString() string {
	return fmt.Sprintf("<PageHeader pageId=%v payloadPageId=%v dataLen=%v>", h.pageId, h.payloadPageId, h.dataLen)
}

func _HashKey(key string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32() & 0x1fff
}

func (db *DB) ToString() string {
	return fmt.Sprintf("<DB path=%v>", db.path)
}

func OpenHash(path string) (*DB, error) {
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
	if err != nil  {
		db.Close()
		return nil, err
	}
	//fmt.Println(db, err, fullpath)
	
	db.header = _ReadHeader(db.file)
	db.hashNodes = make(map[uint32]*HashNode)
	db.nonClusteredNodes = make(map[uint32]*NonClusteredNode)

	db.keyPages = make(map[uint32]*KeyPage)
	db.valPages = make(map[uint32]*ValPage)

	//fmt.Println(strings.Repeat("-", 30))

	fmt.Println("OPEN", db.header.ToString())

	return db, nil
}

func (db *DB) Close() {
	db.lock.Lock()
	defer db.lock.Unlock()

	if db.file != nil {
		db.file.Close()
		db.file = nil
	}
}

func (i *Item) Key() string {
	return i.key
}

func (i *Item) Value() []byte {
	valPage := i.db._GetValuePageByBranchId(i.info.branchId)
	
	value, ok := valPage.valueByKeyId[i.info.keyId]

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

		var keyPageIds []uint32
		for _, pid := range root.context {
			keyPageIds = append(keyPageIds, pid)
		}

		//sort.Ints(keyPageIds)

		for _, pid := range keyPageIds {

			keyPage, err := db._GetKeyPageByPid(pid)
			if err != nil {
				fmt.Println(">>Items", err)
				os.Exit(1)
			}
			//fmt.Println("Items", "hashKey", hashKey, pid)
			
			for key, info := range keyPage.infoByKey {
				
				//fmt.Println("DB Items", key, len(value))
				item := Item{key: key, info: info, db: db}
				ch <-item
			}
		}

		//fmt.Println("Items END")

		close(ch)
	}	(q)

	return q
}



func (db *DB) _InsertBranch(branchId uint64, valPageId uint32) {
	branchRoot := db._GetBranchRoot()
	db._InsertNonClusteredValue(branchRoot, branchId, valPageId)
}

func (db *DB) Update(data map[string][]byte) {
	fmt.Println(db.ToString(), "UPDATE", len(data))

	for k, v := range data {
		db._Set(k, v)
	}

	db._Save()
}


func (db *DB) Set(key string, value []byte) {
	db._Set(key, value)
	db._Save()
}

func (db *DB) Get(key string) ([]byte, bool) {
	keyPage, err := db._GetKeyPage(key)
	_CheckErr("DB Get", err)
	
	info, ok := keyPage.infoByKey[key]
	//fmt.Println("key", key, "GET KEYPAGE", keyPage.ToString(), "info", info, ok)
	if ok {
		valPage := db._GetValuePageByBranchId(info.branchId)
		//fmt.Println("info", info.keyId, "GET VALPAGE", valPage)
		if valPage != nil {

			val, ok := valPage.valueByKeyId[info.keyId]
			return val, ok
		}

	}

	return nil, false
}

func (db *DB) _Set(key string, value []byte) {

	keyPage, err := db._GetKeyPage(key)
	_CheckErr("DB SET", err)
	info, ok := keyPage.infoByKey[key]

	if !ok {
		
		branchId := db._FindOrCreateSpaceBranchValPage(len(value))
		//fmt.Println("_InsertBranch DONE")

		keyId := db._CreateKeyId()
		info = KeyInfo{isDeleted: false, branchId: branchId, keyId:keyId}
		keyPage.infoByKey[key] = info
		keyPage.changed = true
	}

	valPage := db._GetValuePageByBranchId(info.branchId)
	//fmt.Println(">>>>> _GetValuePageByBranchId", valPage)
	if valPage == nil {
		fmt.Println("DB _Set ValPage cant nil")
		os.Exit(1)
	}

	
	valPage.valueByKeyId[info.keyId] = value
	valPage.changed = true

	spaceList := db._GetSpaceList()
	spaceList.Append(info.branchId, valPage)
}

func (db *DB) _Save() {
	//fmt.Println(strings.Repeat("-", 30))
	//fmt.Println("SAVE", db.ToString())

	var pageData []byte
	var err error

	f := db.file

	for _, node := range db.nonClusteredNodes {
		if node.changed {
			node.changed = true
			//fmt.Println("SAVE", pid, node.ToString())
			pageData, err = _Dumps(node)
			_CheckErr("DB Save", err)
			db._SavePayloadData(node.pid, pageData)
		}
	}

	for _, node := range db.hashNodes {
		if node.changed {
			node.changed = true
			//fmt.Println("SAVE",pid, node.ToString())
			pageData, err = _Dumps(node)
			_CheckErr("DB Save", err)
			db._SavePayloadData(node.pid, pageData)
		}
	}

	for pid, keyPage := range db.keyPages {
		if keyPage.changed {
			//fmt.Println(strings.Repeat("-", 30))
			//fmt.Println("** KEYPAGE SAVE **", pid)
			keyPage.changed = false
			pageData, err = _Dumps(keyPage)
			_CheckErr("DB Save", err)
			//fmt.Println(">> KEYPAGE SAVE", keyPage.rowIdByKey)
			db._SavePayloadData(pid, pageData)
		}
	}

	for pid, page := range db.valPages {
		if page.changed {
			//fmt.Println(strings.Repeat("-", 30))
			//fmt.Println("** KEYPAGE SAVE **", pid)
			page.changed = false
			pageData, err = _Dumps(page)
			_CheckErr("DB Save", err)
		
			//fmt.Println(">> KEYPAGE SAVE", pageData)
			db._SavePayloadData(pid, pageData)
		}
	}

	if db.spaceList != nil {
		if db.spaceList.changed {
			db.spaceList.changed = false
			pageData, err = _Dumps(db.spaceList)
			_CheckErr("DB Save", err)
			//fmt.Println("SAVE", db.spaceList.ToString())	
			db._SavePayloadData(db.spaceList.pid, pageData)

		}
	}

	//fmt.Println("SAVE header.......", db.header.ToString())

	headerData := _PackHeader(db.header, db.byteOrder)

	//fmt.Println(strings.Repeat("-", 30))
	//fmt.Println("SAVE header", headerData)

	f.Seek(0, os.SEEK_SET)
	f.Write(headerData)
	f.Sync()
}

func (db *DB) _FindOrCreateSpaceBranchValPage(sizeRequired int) uint64 {

	var branchId uint64

	spaceList := db._GetSpaceList()
	branchId, ok := spaceList.Find(sizeRequired)
	//fmt.Println("_FindOrCreateSpaceBranchValPage", spaceList.ToString(), branchId, ok)

	if ok {
		return branchId
	}

	branchId = db._CreateBranchId()
	newValPage := db._CreateValPage()
	db._InsertBranch(branchId, newValPage.pid)



	return branchId
}


func (db *DB) _GetSpaceList() *SpaceList {

	if db.spaceList == nil {

		var spaceList *SpaceList
		
		if db.header.spaceListPageId == 0 {
			pid := db._CreatePageId()
			spaceList = _NewSpaceList(pid)	
			spaceList.changed = true
			
			db.header.spaceListPageId = spaceList.pid	
		} else {
			
			dataBytes, err := db._LoadPayloadPage(db.header.spaceListPageId)
			
			if err != nil {
				fmt.Println("_GetSpaceList ERROR", err)
				os.Exit(1)
			}
	
			_spaceList, err := _Loads(dataBytes, db.header.spaceListPageId, PGTYPE_SPACELIST)

			spaceList = _spaceList.(*SpaceList)
		}

		db.spaceList  = spaceList
	}

	return db.spaceList
}

func (db *DB) _CreatePageId() uint32 {
	pid := db.header.lastPageId + 1
	db.header.lastPageId = pid
	return pid
}

func (db *DB) _CreateKeyId() uint64 {
	bid := db.header.lastKeyId + 1
	db.header.lastKeyId = bid
	return bid
}

func (db *DB) _CreateBranchId() uint64 {
	bid := db.header.lastBranchId + 1
	db.header.lastBranchId = bid
	return bid
}

func _NewKeyPage() *KeyPage {
	
	keyPage := new(KeyPage)
	keyPage.pid = 0
	keyPage.infoByKey = make(map[string]KeyInfo)

	return keyPage
}

func _NewValPage() *ValPage {
	
	valPage := new(ValPage)
	valPage.pid = 0
	valPage.valueByKeyId = make(map[uint64][]byte)

	return valPage
}

func (p *ValPage) CalcSize() int {

	var count int
	count = PAGE_HEADER_SIZE

	for _, val := range p.valueByKeyId {
		//fmt.Println("CalcSize", p.pid, len(val))
		count += 4
		count += len(val)
	}
	//fmt.Println("CalcSize count", len(p.valueByKeyId))

	return count
}


func (db *DB) _CreateKeyPage() *KeyPage {

	keyPage := _NewKeyPage()
	keyPage.pid = db._CreatePageId()
	keyPage.changed = true

	return keyPage
}

func (db *DB) _CreateValPage() *ValPage {

	valPage := _NewValPage()
	valPage.pid = db._CreatePageId()
	valPage.changed = true
	db.valPages[valPage.pid] = valPage

	return valPage
}


func (db *DB) _WritePageData(pid uint32, data []byte) {
	db.lock.Lock()
	defer db.lock.Unlock()

	if len(data) != PAGE_SIZE {
		fmt.Printf("_WritePageData size needs %v is not %v\n", PAGE_SIZE, len(data))
		os.Exit(1)
	}

	header := _LoadPageHeder(data[:PAGE_HEADER_SIZE])
	if header.pageId != pid {
		fmt.Printf("_WritePageData Error! header.pageId=%v pid=%v\n", header.pageId, pid)
		os.Exit(1)
	}

	seek2 := db._CalcOffsetByPid(pid)

	db.file.Seek(seek2, os.SEEK_SET)
	db.file.Write(data)
}

func (db *DB) _CalcOffsetByPid(pid uint32) int64 {
	return int64(pid) * int64(PAGE_SIZE)
}


func (db *DB) _LoadPageData(pid uint32, size int) (int, error, []byte) {
	db.lock.Lock()
	defer db.lock.Unlock()

	f := db.file

	pageData := make([]byte, size)
	seek2 := db._CalcOffsetByPid(pid)

	f.Seek(seek2, os.SEEK_SET)
	rtn, err := f.Read(pageData)
	//sfmt.Println("_LoadPageData pid=", pid, "seek2", seek2)
	return rtn, err, pageData
}

func (db *DB) _LoadPage(pid uint32) (*Page, error) {

	if pid < 1 {
		return nil, DBError{message: fmt.Sprintf("LOAD PAGE ERROR pid=%v", pid)}
	}

	rtn, err, pageData := db._LoadPageData(pid, PAGE_SIZE)

	if err != nil {
		return nil, err
	}
	if rtn != PAGE_SIZE {
		return nil, DBError{message: fmt.Sprintf("LOAD PAGE ERROR pid=%v readSize==%v\n", pid, rtn)}
	}

	pageHeader := _LoadPageHeder(pageData[:PAGE_HEADER_SIZE])
	if pageHeader.pageId != pid {
		return nil, DBError{message: fmt.Sprintf("LOAD PAGE ERROR pid=%v headerPageId=%v\n", pid, pageHeader.pageId)}
	}
	pageData = pageData[PAGE_HEADER_SIZE:(PAGE_HEADER_SIZE+int(pageHeader.dataLen))]

	page := new(Page)
	page.pid = pid
	page.header = pageHeader
	page.data = pageData
	return page, nil
}

func (db *DB) _GetKeyRoot() *HashNode {

	if db.header.keyRootPageId == 0 {
		node := db._CreateHashNode(0x7fff)		
		db.header.keyRootPageId = node.pid	
	}

	node, ok := db._GetHashNode(db.header.keyRootPageId)

	if !ok {
		fmt.Println("_GetKeyRoot no ok! logic err")
		os.Exit(1)
	}

	return node
}

func (db *DB) _CreateNonClusteredIndex(depth uint8) *NonClusteredNode {
	if depth == 1 {
		return db._CreateNonClusteredNode(NON_CLUSTERED_DATA, 0)
	}

	var stack []*NonClusteredNode
	var parent *NonClusteredNode

	curDepth := depth

	for  {
		curDepth -= 1

		if curDepth < 1 {
			break
		}

		node := db._CreateNonClusteredNode(NON_CLUSTERED_BRANCH, curDepth)
		stack = append(stack, node)
		if parent != nil {
			parent.context[uint64(0)] = uint64(node.pid)
		}

		parent = node
		
	}

	node := db._CreateNonClusteredNode(NON_CLUSTERED_DATA, 0)
	parent.context[0] = uint64(node.pid)
	stack = append(stack, node)

	return stack[0]
}

func (db *DB) _GetBranchRoot() *NonClusteredNode {

	var node *NonClusteredNode

	if db.header.branchRootPageId == 0 {
		node = db._CreateNonClusteredIndex(3)
		db.header.branchRootPageId = node.pid
		db.nonClusteredNodes[node.pid] = node		
	}

	node, ok := db._GetNonClusteredNode(db.header.branchRootPageId)
	if !ok {
		fmt.Println("!!! NO OK LOGIC ERROR")
		os.Exit(1)
	}

	if node == nil {
		fmt.Println("_GetBranchRoot root is nil")
		os.Exit(1)
	}

	return node
}

func (db *DB) _GetValuePageByBranchId(branchId uint64) *ValPage {
	branchRoot := db._GetBranchRoot()
	valPageId, ok := db._GetNonClusteredValue(branchRoot, branchId)

	if ok {
		valPage, err := db._GetValPageByPid(uint32(valPageId))
		if err != nil {
			fmt.Println("_GetValuePageByBranchId branchId=", branchId, valPageId, ok)

		}
		_CheckErr("_GetValuePageByBranchId", err)
		
		return valPage
	}

	return nil
}


func (db *DB) _GetKeyPageByPid(pid uint32) (*KeyPage, error) {
	var keyPage *KeyPage

	keyPage, ok := db.keyPages[pid]
	if !ok {
		//fmt.Println("** LOAD KEYPAGE **")
		dataBytes, err := db._LoadPayloadPage(pid)

		if err != nil {
			return nil, err
		}
		//fmt.Println("LOAD KEYPAGE SUCCESS:", len(dataBytes))
		_keyPage, err := _Loads(dataBytes, pid, PGTYPE_KEY_PAGE)
		if err != nil {
			return nil, err
		}

		keyPage = _keyPage.(*KeyPage)
		db.keyPages[pid] = keyPage

		return  keyPage, nil
	}

	return keyPage, nil
}


func (db *DB) _GetValPageByPid(pid uint32) (*ValPage, error) {
	var valPage *ValPage

	valPage, ok := db.valPages[pid]
	if !ok {
		
		dataBytes, err := db._LoadPayloadPage(pid)
		if err != nil {
			return nil, err
		}
		_valPage, err := _Loads(dataBytes, pid, PGTYPE_VAL_PAGE)
		if err != nil {
			return nil, err
		}
		valPage = _valPage.(*ValPage)
		db.valPages[pid] = valPage		
	}

	return valPage, nil

}

func (db *DB) _GetKeyPage(key string) (*KeyPage, error) {

	hashKey := _HashKey(key)
	root := db._GetKeyRoot()
	
	pid, ok := root.context[hashKey]

	if !ok {
		//fmt.Println("!ok", pid, ok)
		keyPage := db._CreateKeyPage()

		root.context[hashKey] = keyPage.pid
		root.changed = true
		pid = keyPage.pid

		db.keyPages[pid] = keyPage
		return keyPage, nil
	}

	return db._GetKeyPageByPid(pid)
}

