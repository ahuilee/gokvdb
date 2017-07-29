package gokvdb

import (
	"os"
	"fmt"
	"sync"
	"path/filepath"
	//"strings"
)

const (
	PGTYPE_FREELIST byte = 1
	PGTYPE_PAYLOAD byte = 2

	HEADER_SIZE int = 512
	STREAM_PAGER_HEADER_SIZE int = 256
	PAYLOAD_HEADER_SIZE int = 16
	PAYLOAD_PAGE_HEADER_SIZE int = 16
)

type IStream interface {
	Write(data []byte)
	Read(count int) ([]byte, error)
	Seek(offset int64)
	Sync()
	Close()
	ToString() string
}

type IPager interface {
	ReadPage(pid uint32, count int) ([]byte, error)
	WritePage(pid uint32, data []byte)
	CreatePageId() uint32
	FreePageId(pid uint32)
	WritePayloadData(pid uint32, data []byte)
	ReadPayloadData(pid uint32) ([]byte, error)
	Save() []byte
	GetPageSize() int
	ToString() string 
}


type FileStream struct {
	path string
	file *os.File
	rwlock sync.Mutex
}




type BaseStreamPager struct {
	stream IStream
	meta *StreamPagerMeta
	isChanged bool
}

type StreamPagerMeta struct {
	pageSize uint32
	lastPageId uint32
	freelistPageId uint32
}
 
type StreamPager struct {
	basePager *BaseStreamPager
	freelist *FreePageList
	
	payloadFactory *PayloadPageFactory
}



func (s *FileStream) ToString() string {
	return fmt.Sprintf("<FileStream path=%v", s.path)
}

func (s *FileStream) Write(data []byte) {
	s.rwlock.Lock()
	defer s.rwlock.Unlock()
	s.file.Write(data)

}

func (s *FileStream) Read(count int) ([]byte, error) {
	s.rwlock.Lock()
	defer s.rwlock.Unlock()

	data := make([]byte, count)
	_, err := s.file.Read(data)
	if err != nil {
		return nil, err
	}
	//fmt.Println("Read", rtn)

	return data, err
}

func (s *FileStream) Seek(offset int64) {
	s.rwlock.Lock()
	defer s.rwlock.Unlock()
	s.file.Seek(offset, os.SEEK_SET)
}

func (s *FileStream) Sync() {
	s.rwlock.Lock()
	defer s.rwlock.Unlock()
	s.file.Sync()
}

func (s *FileStream) Close() {
	s.rwlock.Lock()
	defer s.rwlock.Unlock()
	if s.file != nil {
		s.file.Sync()
		s.file.Close()
		s.file = nil
	}
}

func OpenFileStream(path string) (IStream, error) {

	fullpath, _ := filepath.Abs(path)
	_CheckCreateDirpath(fullpath)

	f, err := os.OpenFile(fullpath, os.O_RDWR | os.O_CREATE, 0666)
	if err != nil  {
		return nil, err
	}

	stream := new(FileStream)
	stream.path = path
	stream.file = f

	return stream, nil
}

func (meta StreamPagerMeta) ToString() string {
	return fmt.Sprintf("<StreamPagerMeta pageSize=%v lastPageId=%v freelistPageId=%v>", meta.pageSize, meta.lastPageId, meta.freelistPageId)
}


func ReadOrNewStreamPagerMeta(pageSize uint32, data []byte) *StreamPagerMeta {
	//fmt.Println("_ReadOrNewStreamRawPagerMeta", pageSize, data)
	meta := new(StreamPagerMeta)

	if data != nil && len(data) > 8 {

		rd := NewDataStreamFromBuffer(data)
		meta.pageSize = rd.ReadUInt32()
		meta.lastPageId = rd.ReadUInt32()
		meta.freelistPageId = rd.ReadUInt32()
		
	} else {
		meta.pageSize = pageSize
		meta.lastPageId = 0
		meta.freelistPageId = 0
	}

	if meta.pageSize == 0 {
		meta.pageSize = pageSize
	}

	//fmt.Printf("_ReadOrNewStreamRawPagerMeta pageSize=%v lastPageId=%v\n", meta.pageSize, meta.lastPageId)

	return meta
}

func NewStreamPager(stream IStream, meta *StreamPagerMeta) IPager {

	pager := new(StreamPager)
	basePager := new(BaseStreamPager)
	basePager.stream = stream
	basePager.meta = meta

	freelist := _NewFreePageList(basePager, meta.freelistPageId, false)

	pager.basePager = basePager
	pager.freelist = freelist
	pager.payloadFactory = NewPayloadPageFactory(pager)

	basePager.meta.freelistPageId = freelist.rootPageId

	return pager
}

func (p *StreamPager) WritePayloadData(pid uint32, data []byte) {
	
	p.payloadFactory.WritePayloadData(pid, data)
}

func (p *StreamPager) ReadPayloadData(pid uint32) ([]byte, error) {
	return p.payloadFactory.ReadPayloadData(pid)
}	

func (p *StreamPager) GetPageSize() int {
	return int(p.basePager.meta.pageSize)
}

func (p *StreamPager) ToString() string {
	return fmt.Sprintf("<StreamPager meta=%v>", p.basePager.meta.ToString())
}

func (p *StreamPager) Save() []byte {
	p.freelist.Save()

	if p.basePager.isChanged {
		w := NewDataStreamFromBuffer(make([]byte, STREAM_PAGER_HEADER_SIZE))
		w.WriteUInt32(p.basePager.meta.pageSize)
		w.WriteUInt32(p.basePager.meta.lastPageId)
		w.WriteUInt32(p.basePager.meta.freelistPageId)

		//p.stream.Seek(0)
		//p.stream.Write(w.ToBytes())

		p.basePager.isChanged = false

		//fmt.Println(">>>>>>>>>>>>>>>>> StreamRawPager SAVE", "p.meta.lastPageId", p.meta.lastPageId)

		return w.ToBytes()		
	}

	return nil
}

func (p *StreamPager) ReadPage(pid uint32, count int) ([]byte, error) {
	return p.basePager.ReadPage(pid, count)
}

func (p *StreamPager) WritePage(pid uint32, data []byte) {
	p.basePager.WritePage(pid, data)
}

func (p *StreamPager)	CreatePageId() uint32 {
	freeId, ok := p.freelist.Pop()
	if ok {
		return freeId
	}

	return p.basePager.CreatePageId()
}

func (p *StreamPager)	FreePageId(pid uint32) {
	//fmt.Println(p.ToString(), "FreePageId", pid)
	if pid < 1 {
		fmt.Printf("cant free pageId=%v\n", pid)
		os.Exit(1)
	}
	empty := make([]byte, PAYLOAD_PAGE_HEADER_SIZE)
	p.basePager.WritePage(pid, empty)
	p.freelist.Put(pid)
}


func (p *BaseStreamPager) GetPageSize() int {
	return int(p.meta.pageSize)
}

func (p *BaseStreamPager) CalcPageOffset(pid uint32) int64 {
	return int64(pid * p.meta.pageSize)
}

func (p *BaseStreamPager) ReadPage(pid uint32, count int) ([]byte, error) {

	if count == 0 || count > int(p.meta.pageSize) {
		count = int(p.meta.pageSize)
	}

	seek2 := p.CalcPageOffset(pid)
	p.stream.Seek(seek2)
	data, err := p.stream.Read(count)

	//fmt.Printf("ReadPage pid=%v pageSize=%v count=%v seek=%v dataLen=%v\n", pid, p.meta.pageSize, count, seek2, len(data))

	return data, err
}


func (p *BaseStreamPager) WritePage(pid uint32, data []byte) {
	if len(data) > int(p.meta.pageSize) {
		fmt.Printf("page size needs <= %v\n", p.meta.pageSize)
		os.Exit(1)
	}

	pageData := make([]byte, p.meta.pageSize)
	copy(pageData, data)

	seek2 := p.CalcPageOffset(pid)
	p.stream.Seek(seek2)
	p.stream.Write(pageData)

	//fmt.Printf("WritePage pid=%v seek=%v dataLen=%v\n", pid, seek2, len(data))
}
	
func (p *BaseStreamPager)	CreatePageId() uint32 {
	pid := p.meta.lastPageId + 1
	p.meta.lastPageId = pid
	p.isChanged = true

	//fmt.Println("======================StreamRawPager CreatePageId", pid, "meta", p.meta.lastPageId )

	return pid
}

func (p *BaseStreamPager)	FreePageId(pid uint32) {
}



func (p *BaseStreamPager) ToString() string {
	return fmt.Sprintf("<BaseStreamPager lastPageId=%v>", p.meta.lastPageId)
}

func (p *BaseStreamPager) Save() []byte {
	fmt.Println("NO Implemented Save")	
	os.Exit(1)
	return nil
}

func (p *BaseStreamPager) WritePayloadData(pid uint32, data []byte) {
	fmt.Println("NO Implemented WritePayloadData")	
	os.Exit(1)
}

func (p *BaseStreamPager) ReadPayloadData(pid uint32) ([]byte, error) {
	fmt.Println("NO Implemented ReadPayloadData")	
	os.Exit(1)
	return nil, DBError{message: "NO Implemented ReadPayloadData"}
}	


type PayloadPageFactory struct {
	pager IPager
	isDebug bool
}

func NewPayloadPageFactory(pager IPager) *PayloadPageFactory {

	factory := new(PayloadPageFactory)
	factory.pager = pager

	return factory
}


type PayloadPageWriter struct {
	pageIds []uint32
	pageIdIndex int
	pager IPager
	factory *PayloadPageFactory
}

type PayloadPageHeader struct {
	//pgType uint8
	pageIndex int
	contentLen uint32
	hasNextPage bool
	nextPageId uint32
}

func _ReadPayloadPageHeader(rd *DataStream) PayloadPageHeader {

	
	pageIndex := int(rd.ReadUInt16())
	contentLen :=	rd.ReadUInt32() 
	hasNextPage := rd.ReadBool() 
	nextPageId := rd.ReadUInt32()

	hdr := PayloadPageHeader{ pageIndex: pageIndex, contentLen: contentLen, hasNextPage: hasNextPage, nextPageId: nextPageId}

	return hdr
}

func (w *PayloadPageWriter) CalcPageIds(pid uint32) {	

	//fmt.Println("CalcPageIds pid", pid)

	var pageIds []uint32

	var curPageId uint32
	var nextPageId uint32

	nextPageId = pid

	//fmt.Println("PayloadPageWriter CalcPageIds")
	for {
		//fmt.Println("PayloadPageWriter CalcPageIds", nextPageId)

		if curPageId == nextPageId {
			fmt.Printf("ERROR curPageId=%v nextPageId=%v\n", curPageId, nextPageId)
			os.Exit(1)

			break
		}

		curPageId = nextPageId

		if curPageId < 1 {
			fmt.Printf("ERROR payloadPageId=%v\n", curPageId)
			os.Exit(1)
		}

		pageIds = append(pageIds, curPageId)

		headerBytes, err := w.pager.ReadPage(curPageId, PAYLOAD_PAGE_HEADER_SIZE)

		if w.factory.isDebug {
			fmt.Printf("[CalcPageIds] rootId=%v curPageId=%v err=%v headerBytes=%v\n", pid, curPageId, err, headerBytes)
		}
		//
		if err == nil && len(headerBytes) == PAYLOAD_PAGE_HEADER_SIZE  {
			hdrR := NewDataStreamFromBuffer(headerBytes)
			pgType := hdrR.ReadUInt8()

			/*
			if w.factory.isDebug {
				fmt.Println("pid", curPageId, "pgType", pgType)
			}
			*/
			if pgType != PGTYPE_PAYLOAD {
				break
			}
			hdr := _ReadPayloadPageHeader(hdrR)


			nextPageId = hdr.nextPageId
			//fmt.Println(">>>>>>>> pageHeaderNextPageId =..", pageHeaderNextPageId)
			if hdr.hasNextPage {
				continue
			}
		} 

		break
	}

	w.pageIdIndex = 0
	w.pageIds = pageIds

}

func (w *PayloadPageWriter) GetOrCreateNextWritePageId() uint32 {	

	if w.pageIdIndex < len(w.pageIds) {
		pid := w.pageIds[w.pageIdIndex]
		w.pageIdIndex += 1
		return pid
	}

	return w.pager.CreatePageId()
}

func (w *PayloadPageWriter) FreePageIds() {


	for {

		if !(w.pageIdIndex < len(w.pageIds)) {
			break
		}
		pid := w.pageIds[w.pageIdIndex]
		//fmt.Println("PayloadPageWriter FreePageIds", pid)
		if pid == 0 {
			fmt.Println("FreePageIds cant == 0")
			os.Exit(1)
		}
		w.pager.FreePageId(pid)
		w.pageIdIndex += 1
	}

}

func (w *PayloadPageWriter) Write(pid uint32, data []byte) {
	w.CalcPageIds(pid)

	ds := NewDataStream()
	ds.WriteUInt32(uint32(len(data)))
	ds.Seek(PAYLOAD_HEADER_SIZE)	
	ds.Write(data)		
	//

	writeData := ds.ToBytes()

	var pageIndex uint16
	var curPageId uint32
	var nextPageId uint32
	var iStart int
	var iEnd int

	nextPageId = pid

	iStart = 0
	loopCount := 0

	pageSize := w.pager.GetPageSize()
	pageContentSize := pageSize - PAYLOAD_PAGE_HEADER_SIZE

	curPageId = 0

	nextPageId = w.GetOrCreateNextWritePageId()	

	writeDataLen := len(writeData)

	pageIndex = 0
	

	for {

		if !(iStart < writeDataLen) {
			break
		}

		loopCount += 1

		curPageId = nextPageId		

		hasNextPage := false

		iEnd = iStart + pageContentSize

		if iEnd > writeDataLen {
			iEnd = writeDataLen
		} 

		if iEnd < writeDataLen {
			hasNextPage = true			
		}

		if hasNextPage {
			nextPageId = w.GetOrCreateNextWritePageId()	
		}	else {
			nextPageId = 0
			//fmt.Println("!hasNextPage nextPageId = 0")
		}

		if hasNextPage && nextPageId < 1 {
			fmt.Println("Error nextPageId", nextPageId)
			os.Exit(1)
		}

		pageContentData := writeData[iStart:iEnd]

		pageContentDataLen := uint32(len(pageContentData))


		if w.factory.isDebug {

			fmt.Println("[PayloadPageWriter] pageIndex", pageIndex, "pageContentDataLen", pageContentDataLen, "hasNextPage", hasNextPage, "curPageId", curPageId, "nextPageId", nextPageId)

		}


		pageW := NewDataStream()
		pageW.WriteUInt8(PGTYPE_PAYLOAD)
		pageW.WriteUInt16(pageIndex)
		pageW.WriteUInt32(pageContentDataLen)
		pageW.WriteBool(hasNextPage)
		pageW.WriteUInt32(nextPageId)

		pageW.Seek(PAYLOAD_PAGE_HEADER_SIZE)

		pageW.Write(pageContentData)
		//pageBuf.Seek(PAGE_HEADER_SIZE)
		pageData := pageW.ToBytes()
		if len(pageData) > pageSize {
			pageData = pageData[:pageSize]
		}

		testRd := NewDataStreamFromBuffer(pageData)
		testRd.ReadUInt8()
		testRd.ReadUInt16()
		testContentLen := testRd.ReadUInt32()

		w.pager.WritePage(curPageId, pageData)

		//fmt.Println("SAVE pageData", "loopCount", loopCount, "pid=", curPageId, "len", len(pageData), "testContentLen", testContentLen, len(pageContentData))

		if len(pageContentData) != int(testContentLen) {
			fmt.Println("TEST CONTENT LEN ERROR!", pageContentDataLen, testContentLen, int(testContentLen))
			os.Exit(1)
		}


		iStart += pageContentSize
		pageIndex += 1
	}

	w.FreePageIds()
}


func (f *PayloadPageFactory) WritePayloadData(pid uint32, data []byte) {
	if pid < 1 {
		fmt.Println("WritePayloadData ERROR pid=", pid)
		os.Exit(1)
	}

	w := new(PayloadPageWriter)
	w.pager = f.pager
	w.factory = f
	w.Write(pid, data)
}



func (f *PayloadPageFactory) 	ReadPayloadData(pid uint32) ([]byte, error) {

	//fmt.Println("_LoadPayloadPage", pid)
	var allBytes []byte

	curPageId := pid
	pageCount := 0

	for {
		/*
		if curPageId == 0 {
			break
		}*/ 
		pageCount += 1
		//fmt.Println("ReadPayloadData", "rootPid", pid, "pageCount", pageCount, "curPageId", curPageId)
		pageData, err := f.pager.ReadPage(curPageId, 0)
		if err != nil {
			return nil, err
		}


		rd := NewDataStreamFromBuffer(pageData)
		pgType := rd.ReadUInt8()
		header := _ReadPayloadPageHeader(rd)
		if pgType != PGTYPE_PAYLOAD {
			fmt.Println("ERROR PayloadPage pgType=", pgType)
			os.Exit(1)
		}

		//fmt.Printf("Read PayloadPage rootPid=%v pageIndex=%v pageDataLen=%v hasNextPage=%v curPageId=%v nextPageId=%v\n", pid, header.pageIndex, header.contentLen, header.hasNextPage, curPageId,  header.nextPageId )
		

		rd.Seek(PAYLOAD_PAGE_HEADER_SIZE)

		contentBytes := rd.Read(int(header.contentLen))
		//fmt.Println("_LoadPayloadPage", "pid=", nextPageId, "contentBytes=", len(contentBytes))
		allBytes = append(allBytes, contentBytes...)


		if !header.hasNextPage {
			break
		}

		curPageId = header.nextPageId
	}

	if len(allBytes) < PAYLOAD_HEADER_SIZE {
		return nil, DBError{message: fmt.Sprintf("pid=%v bytes=%v not enough!", pid, len(allBytes))}
	}

	buf := NewDataStreamFromBuffer(allBytes)
	payloadDataLenRequired := buf.ReadUInt32()
	//fmt.Println("pid=", pid, "payloadDataLenRequired", payloadDataLenRequired)
	allBytes = allBytes[PAYLOAD_HEADER_SIZE:]

	if uint32(len(allBytes)) != payloadDataLenRequired {
		return nil, DBError{message: fmt.Sprintf("bytes length needs %v payload data is %v!", payloadDataLenRequired, len(allBytes))}
	}

	return allBytes, nil
}
