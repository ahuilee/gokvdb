package gokvdb


import (
	"os"
	"fmt"

)


type FreePageList struct {
	rootPageId uint32
	pageIdSet map[uint32]byte

	pager IPager
}



func _NewFreePageList(pager IPager, rootPageId uint32, isDeubg bool) *FreePageList {

	list := new(FreePageList)
	list.pager = pager
	list.pageIdSet = make(map[uint32]byte)

	//fmt.Println("_NewFreePageList rootPageId=", rootPageId)

	if rootPageId == 0 {
		rootPageId = pager.CreatePageId()
	} else {

		data, err := _FreeListReadPayloadData(pager, rootPageId)

		if err == nil {
			rd := NewDataStreamFromBuffer(data)
			rowsCount := int(rd.ReadUInt32())
			for i:=0; i<rowsCount; i++ {
				pid := rd.ReadUInt32()
				list.pageIdSet[pid] = 1

				if isDeubg {

					fmt.Println("READ FREE PAGE", pid)
				}
			}
		}

	}

	list.rootPageId = rootPageId	

	return list
}

func (fl *FreePageList) Put(pid uint32) {
	fl.pageIdSet[pid] = 1
	//fmt.Println("FreePageList Put", pid)
}

func (fl *FreePageList) Pop() (uint32, bool) {

	for k, _ := range fl.pageIdSet {
		delete(fl.pageIdSet, k)
		//fmt.Println("FreePageList Pop", k)
		return k, true
	}

	return 0, false
}

func (fl *FreePageList) Save() {

	w := NewDataStream()
	w.WriteUInt32(uint32(len(fl.pageIdSet)))
	for pid, _ := range fl.pageIdSet {
		w.WriteUInt32(pid)
	}

	//fmt.Println("FreePageList Save", len(fl.pageIdSet))

	_FreeListWritePayloadData(fl.pager, fl.rootPageId, w.ToBytes())

}


func _FreeListWritePayloadData(pager IPager, pid uint32, data []byte) {
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
			pgType := hdrR.ReadUInt8() // pgType
			if pgType == PGTYPE_FREELIST {
				hdrR.ReadUInt32() // pageHeaderContentLen
				hdrR.ReadBool() // pageHeaderHasNextPage
				pageHeaderNextPageId = hdrR.ReadUInt32()
			} else {
				pageHeaderNextPageId = 0
			}
			
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
		pageW.WriteUInt8(PGTYPE_FREELIST)
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
			testRd.ReadUInt8()
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

func _FreeListReadPayloadData(pager IPager, pid uint32) ([]byte, error) {

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
		pageData, err := pager.ReadPage(nextPageId, 0)
		if err != nil {
			return nil, err
		}

		pageDataLen = 0
		rd := NewDataStreamFromBuffer(pageData)
		pgType := rd.ReadUInt8()

		if pgType != PGTYPE_FREELIST {
			fmt.Println("_FreeListReadPayloadData pgType!=PGTYPE_FREELIST", pgType)
			os.Exit(1)
		}
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
