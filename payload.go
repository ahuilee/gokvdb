package gokvdb

import (
	"os"
	"fmt"
)

const PAYLOAD_HEADER_SIZE int = 16

func _CheckErr(message string, err error) {
	if err != nil {
		fmt.Println(message, "ERROR", err)
		os.Exit(1)
	}
}


func (db *DB) _LoadPayloadPage(pid uint32) ([]byte, error) {

		//fmt.Println("_LoadPayloadPage", pid)
		var allBytes []byte
		nextPageId := pid
		pageCount := 0

		for {
			if nextPageId == 0 {
				break
			}

			pageCount += 1
			//fmt.Println("_LoadPayloadPage loopCount", loopCount, "pid", pid)
			page, err := db._LoadPage(nextPageId)
			if err != nil {
				return nil, err
			}
			if page == nil {
				break
			}
			//fmt.Println("_LoadPayloadPage", "page=", page.ToString())
			allBytes = append(allBytes, page.data...)
			nextPageId = page.header.payloadPageId
		}

		if len(allBytes) < PAYLOAD_HEADER_SIZE {
			return nil, DBError{message: fmt.Sprintf("pid=%v bytes not enough!", pid)}
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



func (db *DB) _SavePayloadData(pid uint32, data []byte) {
	//fmt.Println("_SavePayloadData", "pid=", pid)

	if pid < 1 {
		fmt.Println("_SavePayloadData ERROR pid=", pid)
		os.Exit(1)
	}

	ds := NewDataStream()
	ds.WriteUInt32(uint32(len(data)))
	ds.Seek(PAYLOAD_HEADER_SIZE)	
	ds.Write(data)

	//fmt.Println("_SavePayloadData pid=", pid, "contentLen", len(data))

	writeData := ds.ToBytes()

	var curPageId uint32
	var nextPageId uint32
	var iStart int
	var iEnd int

	nextPageId = pid

	iStart = 0

	loopCount := 0

	for {
		if iStart >= len(writeData) {
			break
		}

		loopCount += 1

		curPageId = nextPageId

		var pageHeader PageHeader

		_, err, pageHeaderData := db._LoadPageData(curPageId, PAGE_HEADER_SIZE)
		if err == nil {
			pageHeader = _LoadPageHeder(pageHeaderData)
			
		} else {
			pageHeader = PageHeader{payloadPageId: 0, dataLen: 0}
		}
		pageHeader.pageId = curPageId

		//fmt.Println("_SavePayloadData loop", loopCount, pageHeader.ToString())

		iEnd = iStart + PAGE_CONENT_SIZE

		if iEnd >= len(writeData) {
			iEnd = len(writeData)
		} else {
			if pageHeader.payloadPageId == 0 {
				pageHeader.payloadPageId = db._CreatePageId()
			}
		}

		//fmt.Println("iStart", iStart, "iEnd", iEnd)

		pageContentData := writeData[iStart:iEnd]

		//fmt.Println("pageContentData", len(pageContentData))

		pageHeader.dataLen = uint16(len(pageContentData))
		pageHeaderData = _PackPageHeader(pageHeader, db.byteOrder)

		//fmt.Println("pageHeaderData", len(pageHeaderData))

		if len(pageHeaderData) != PAGE_HEADER_SIZE {
			fmt.Println(fmt.Sprintf("len(pageHeaderData) != %v", PAGE_HEADER_SIZE))
			os.Exit(1)
		}

		pageBuf := NewDataStreamFromBuffer(make([]byte, PAGE_SIZE))

		pageBuf.Write(pageHeaderData)
		//pageBuf.Seek(PAGE_HEADER_SIZE)
		pageBuf.Write(pageContentData)
		pageData := pageBuf.ToBytes()

		db._WritePageData(curPageId, pageData)
	

		//fmt.Println("SAVE pageData", "loopCount", loopCount, "pid=", curPageId, "len", len(pageData))
		//fmt.Println(pageData)
		//fmt.Println("pageContentData", pageContentData)

	
		nextPageId = pageHeader.payloadPageId
		iStart = iEnd
	}
}

