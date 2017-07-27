package gokvdb

import (
	"os"
	"fmt"
	"sync"
	"path/filepath"
	//"strings"
)

const (
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
	WritePayloadData(pid uint32, data []byte)
	ReadPayloadData(pid uint32) ([]byte, error)
	Save() []byte
	GetPageSize() int
	ToString() string 
}

func _CheckCreateDirpath(fullpath string) {

	dirpath := filepath.Dir(fullpath)
	_, err := os.Stat(dirpath)
	if err != nil {
		if os.IsNotExist(err) {
			os.MkdirAll(dirpath, os.ModePerm)
		}
	}
}


type FileStream struct {
	path string
	file *os.File
	rwlock sync.Mutex
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
	return fmt.Sprintf("<StreamPagerMeta pageSize=%v lastPageId=%v>", meta.pageSize, meta.lastPageId)
}


func ReadOrNewStreamPagerMeta(pageSize uint32, data []byte) *StreamPagerMeta {
	//fmt.Println("ReadOrNewStreamPagerMeta", pageSize, data)
	meta := new(StreamPagerMeta)

	if data != nil && len(data) > 8 {

		rd := NewDataStreamFromBuffer(data)
		meta.pageSize = rd.ReadUInt32()
		meta.lastPageId = rd.ReadUInt32()
		
	} else {
		meta.pageSize = pageSize
		meta.lastPageId = 0
	}

	if meta.pageSize == 0 {
		meta.pageSize = pageSize
	}

	//fmt.Printf("ReadOrNewStreamPagerMeta pageSize=%v lastPageId=%v\n", meta.pageSize, meta.lastPageId)

	return meta
}

//func NewStreamPager(stream IStream, pageSize uint32) IPager {
func NewStreamPager(stream IStream, meta *StreamPagerMeta) IPager {

	pager := new(StreamPager)
	pager.stream = stream
	pager.payloadFactory = NewPayloadPageFactory(pager)

	pager.meta = meta

	return pager
}

type StreamPagerMeta struct {
	pageSize uint32
	lastPageId uint32
}

type StreamPager struct {
	stream IStream
	meta *StreamPagerMeta
	payloadFactory *PayloadPageFactory
	isChanged bool
}

func (p *StreamPager) WritePayloadData(pid uint32, data []byte) {
	p.payloadFactory.WritePayloadData(pid, data)
}

func (p *StreamPager) ReadPayloadData(pid uint32) ([]byte, error) {
	return p.payloadFactory.ReadPayloadData(pid)
}	

func (p *StreamPager) GetPageSize() int {
	return int(p.meta.pageSize)
}

func (p *StreamPager) ToString() string {
	return fmt.Sprintf("<StreamPager lastPageId=%v>", p.meta.lastPageId)
}

func (p *StreamPager) CalcPageOffset(pid uint32) int64 {
	return int64(pid * p.meta.pageSize)
}

func (p *StreamPager) ReadPage(pid uint32, count int) ([]byte, error) {

	if count == 0 || count > int(p.meta.pageSize) {
		count = int(p.meta.pageSize)
	}

	seek2 := p.CalcPageOffset(pid)
	p.stream.Seek(seek2)
	data, err := p.stream.Read(count)

	//fmt.Printf("ReadPage pid=%v pageSize=%v count=%v seek=%v dataLen=%v\n", pid, p.meta.pageSize, count, seek2, len(data))

	return data, err
}


func (p *StreamPager) WritePage(pid uint32, data []byte) {
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
	
func (p *StreamPager)	CreatePageId() uint32 {
	pid := p.meta.lastPageId + 1
	p.meta.lastPageId = pid
	p.isChanged = true

	//fmt.Println("======================StreamPager CreatePageId", pid, "meta", p.meta.lastPageId )

	return pid
}

func (p *StreamPager) Save() []byte {
	if p.isChanged {
		w := NewDataStreamFromBuffer(make([]byte, STREAM_PAGER_HEADER_SIZE))
		w.WriteUInt32(p.meta.pageSize)
		w.WriteUInt32(p.meta.lastPageId)

		//p.stream.Seek(0)
		//p.stream.Write(w.ToBytes())

		p.isChanged = false

		//fmt.Println(">>>>>>>>>>>>>>>>> StreamPager SAVE", "p.meta.lastPageId", p.meta.lastPageId)

		return w.ToBytes()		
	}

	return nil
}

type PayloadPageFactory struct {
	pager IPager
}

func NewPayloadPageFactory(pager IPager) *PayloadPageFactory {

	factory := new(PayloadPageFactory)
	factory.pager = pager

	return factory
}


func (f *PayloadPageFactory) WritePayloadData(pid uint32, data []byte) {
	if pid < 1 {
		fmt.Println("WritePayloadData ERROR pid=", pid)
		os.Exit(1)
	}

	/*
	logF, err := os.OpenFile("./log_paylog.txt", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	fmt.Println("OPEN LOG", err)
	defer logF.Close()
	*/

	ds := NewDataStream()
	ds.WriteUInt32(uint32(len(data)))
	ds.Seek(PAYLOAD_HEADER_SIZE)	
	ds.Write(data)
	//

	writeData := ds.ToBytes()
	//fmt.Println("_SavePayloadData pid=", pid, "contentLen", len(data))

	//logF.WriteString(fmt.Sprintf("WritePayloadData pid=%v data=%v\n", pid, len(data)))

	var curPageId uint32
	var nextPageId uint32
	var iStart int
	var iEnd int

	nextPageId = pid

	iStart = 0
	loopCount := 0
	pager := f.pager

	//var pageHeaderContentLen uint16
	//var pageHeaderHasNextPage bool
	var pageHeaderNextPageId uint32

	pageSize := pager.GetPageSize()
	pageContentSize := pageSize - PAYLOAD_PAGE_HEADER_SIZE

	for {
		if iStart >= len(writeData) {
			break
		}

		//fmt.Println(strings.Repeat("-", 30))

		loopCount += 1

		curPageId = nextPageId

		headerBytes, err := pager.ReadPage(curPageId, PAYLOAD_PAGE_HEADER_SIZE)
		//fmt.Println("headerBytes", "curPageId", curPageId, headerBytes, err)
		if err == nil && len(headerBytes) == PAYLOAD_PAGE_HEADER_SIZE  {
			hdrR := NewDataStreamFromBuffer(headerBytes)
			hdrR.ReadUInt32() // pageHeaderContentLen
			hdrR.ReadBool() // pageHeaderHasNextPage
			pageHeaderNextPageId = hdrR.ReadUInt32()
			
			//fmt.Println(">>>>>>>> pageHeaderNextPageId =..", pageHeaderNextPageId)
		} else {
			pageHeaderNextPageId = 0
			//fmt.Println(">>>>>>>>>>>>>>> pageHeaderNextPageId = 0")			
		}		


		iEnd = iStart + pageContentSize
		hasNextPage := true

		if iEnd > len(writeData) {
			iEnd = len(writeData)
		} 

		if iEnd < len(writeData) {
			if pageHeaderNextPageId == 0 {
				pageHeaderNextPageId = pager.CreatePageId()
				//fmt.Println(">>CreatePageId", pageHeaderNextPageId)
			}
			hasNextPage = true
		} else {
			hasNextPage = false
		}

		//fmt.Println("iStart", iStart, "iEnd", iEnd, "pageHeaderNextPageId", pageHeaderNextPageId, "hasNextPage", hasNextPage)
		pageContentData := writeData[iStart:iEnd]

		pageContentDataLen := uint32(len(pageContentData))

		pageW := NewDataStream()
		pageW.WriteUInt32(pageContentDataLen)
		pageW.WriteBool(hasNextPage)
		pageW.WriteUInt32(pageHeaderNextPageId)

		pageW.Seek(PAYLOAD_PAGE_HEADER_SIZE)

		pageW.Write(pageContentData)
		//pageBuf.Seek(PAGE_HEADER_SIZE)
		pageData := pageW.ToBytes()
		if len(pageData) > pageSize {
			pageData = pageData[:pageSize]
		}

		testRd := NewDataStreamFromBuffer(pageData)
		testContentLen := testRd.ReadUInt32()

		pager.WritePage(curPageId, pageData)

		//fmt.Println("SAVE pageData", "loopCount", loopCount, "pid=", curPageId, "len", len(pageData), "testContentLen", testContentLen, len(pageContentData))

		if len(pageContentData) != int(testContentLen) {
			fmt.Println("TEST CONTENT LEN ERROR!", pageContentDataLen, testContentLen, int(testContentLen))
			os.Exit(1)
		}
		//fmt.Println(pageData)
		//fmt.Println("pageContentData", pageContentData)	
		nextPageId = pageHeaderNextPageId
		iStart = iEnd		

		//logF.WriteString(fmt.Sprintf("WritePayloadData loop=%v rootPid=%v curPageId=%v pageData=%v pageContentData=%v hasNextPage=%v nextPageId=%v\n", loopCount, pid, curPageId, len(pageData), len(pageContentData), hasNextPage, nextPageId))
	}

}

func (f *PayloadPageFactory) 	ReadPayloadData(pid uint32) ([]byte, error) {

	//fmt.Println("_LoadPayloadPage", pid)
	var allBytes []byte
	var nextPayloadPageId uint32
	var pageDataLen int
	var hasNextPage bool

	nextPageId := pid
	pageCount := 0

	for {
		if nextPageId == 0 {
			break
		}

		pageCount += 1
		//fmt.Println("ReadPayloadData", "rootPid", pid, "pageCount", pageCount, "readPageId", nextPageId)
		pageData, err := f.pager.ReadPage(nextPageId, 0)
		if err != nil {
			return nil, err
		}

		pageDataLen = 0
		rd := NewDataStreamFromBuffer(pageData)
		pageDataLen = int(rd.ReadUInt32())
		hasNextPage = rd.ReadBool()
		nextPayloadPageId = rd.ReadUInt32()

		//fmt.Printf("Read PayloadPage rootPid=%v pid=%v pageCount=%v pageDataLen=%v hasNextPage=%v nextPid=%v pageDataLen=%v\n", pid, nextPageId, pageCount, pageDataLen, hasNextPage, nextPayloadPageId, pageDataLen)
		

		rd.Seek(PAYLOAD_PAGE_HEADER_SIZE)

		contentBytes := rd.Read(pageDataLen)
		//fmt.Println("_LoadPayloadPage", "pid=", nextPageId, "contentBytes=", len(contentBytes))
		allBytes = append(allBytes, contentBytes...)
		nextPageId = nextPayloadPageId

		if !hasNextPage {
			break
		}
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
