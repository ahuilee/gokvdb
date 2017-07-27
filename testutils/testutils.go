package testutils

import (
	"os"
	"fmt"
	//"time"
	//"bytes"
	"math/rand"
	"../../gokvdb"
	//"github.com/satori/go.uuid"	
)

func CheckErr(err error) {
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}


func OpenStorage(path string, callback func(s *gokvdb.Storage)) {

	s, err := gokvdb.OpenStorage(path)

	if err != nil {
		s.Close()
	}

	CheckErr(err)

	fmt.Println("OpenStorage", s.ToString())

	callback(s)

	s.Close()

}

func RandBytes(count int) []byte {
	data := make([]byte, count)
	for i:=0; i<len(data); i++ {
		data[i] = byte(rand.Intn(255))
	}

	return data
}

func OpenInternalPager(path string, pageSize int, metaOffset int, mode string, callback func(gokvdb.IPager)) {

	stream, err := gokvdb.OpenFileStream(path)
	defer stream.Close()

	if err == nil {

		stream.Seek(int64(metaOffset))
		metaData, _ := stream.Read(128)
		stream.Seek(int64(metaOffset + 128))
		metaData2, _ := stream.Read(128)

		meta := gokvdb.ReadOrNewStreamPagerMeta(uint32(pageSize), metaData)
		pager := gokvdb.NewStreamPager(stream, meta)
		internalPager := gokvdb.NewInternalPager(pager, 128, metaData2)

		fmt.Println("ReadOrNewStreamPagerMeta", meta.ToString())


		callback(internalPager)

		if mode == "w" {

			metaData2 = internalPager.Save()
			
			metaData = pager.Save()
			
			stream.Seek(int64(metaOffset))
			stream.Write(metaData)
			stream.Seek(int64(metaOffset + 128))
			stream.Write(metaData2)
		}
	}
	

}


func OpenStreamPager(path string, pageSize int, metaOffset int, mode string, callback func(gokvdb.IPager)) {

	stream, err := gokvdb.OpenFileStream(path)

	defer stream.Close()

	if err == nil {

		stream.Seek(int64(metaOffset))
		metaData, _ := stream.Read(128)

		meta := gokvdb.ReadOrNewStreamPagerMeta(uint32(pageSize), metaData)
		pager := gokvdb.NewStreamPager(stream, meta)

		fmt.Println("ReadOrNewStreamPagerMeta", meta.ToString())


		callback(pager)

		if mode == "w" {
		
			metaData = pager.Save()
			stream.Seek(int64(metaOffset))
			stream.Write(metaData)		
		}
	}

}


func OpenBTeeBlobMap(path string, pageSize int, mode string, callback func(*gokvdb.BTreeBlobMap)) {

	metaOffset := 0

	stream, err := gokvdb.OpenFileStream(path)
	defer stream.Close()

	if err == nil {

		stream.Seek(int64(metaOffset))
		metaData, _ := stream.Read(128)

		stream.Seek(int64(metaOffset + 128))
		metaData2, _ := stream.Read(128)

		stream.Seek(int64(metaOffset + 256))
		metaData3, _ := stream.Read(128)

		meta := gokvdb.ReadOrNewStreamPagerMeta(uint32(pageSize), metaData)
		pager := gokvdb.NewStreamPager(stream, meta)
		internalPager := gokvdb.NewInternalPager(pager, 128, metaData2)
		btreeMap := gokvdb.NewBTreeBlobMap(internalPager, metaData3)

		callback(btreeMap)



		if mode == "w" {
		
			metaData3 = btreeMap.Save()

			metaData2 = internalPager.Save()
			
			metaData = pager.Save()
			
			stream.Seek(int64(metaOffset))
			stream.Write(metaData)
			stream.Seek(int64(metaOffset + 128))
			stream.Write(metaData2)
			stream.Seek(int64(metaOffset + 256))
			stream.Write(metaData3)
		}


	}


}
