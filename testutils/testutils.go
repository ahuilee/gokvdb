package testutils

import (
	"os"
	"fmt"
	//"time"
	//"bytes"
	"bufio"
	"strconv"
	"strings"
	"math/rand"
	"path/filepath"
	"../../gokvdb"	
	"github.com/satori/go.uuid"	
)

func CheckErr(err error) {
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}



func CreateTempFilePath() string {

	path :=  fmt.Sprintf("./testdata/%v.tmp", uuid.NewV4())

	fullpath, _ := filepath.Abs(path)

	dirPath := filepath.Dir(fullpath)
	_, err := os.Stat(dirPath) 
	if err != nil {
		if os.IsNotExist(err) {
			os.MkdirAll(dirPath, os.ModePerm)
		}
	}

	

	f, _ := os.OpenFile(path, os.O_CREATE, 0666)
	defer f.Close()

	return path
}

func RandomI64StrItems(count int, logPath string ) chan []interface{} {
	
	q := make(chan []interface{})

	go func(ch chan []interface{}, logPath string) {

		f, _ := os.OpenFile(logPath, os.O_RDWR | os.O_APPEND, 0666)
		defer f.Close()

		for i:=0; i<count; i++ {
			key := rand.Int63n(72057594037927936)
			val := fmt.Sprintf("key-%v", uuid.NewV4())

			item := []interface{}{key, val}
			ch <- item

			f.WriteString(fmt.Sprintf("%v\t%v\n", key, val))
		}

		f.Sync()

		close(ch)
	}(q, logPath)

	return q
}

func RandomStrI64Items(count int, logPath string ) chan []interface{} {
	
	q := make(chan []interface{})

	go func(ch chan []interface{}, logPath string) {

		f, _ := os.OpenFile(logPath, os.O_RDWR | os.O_APPEND, 0666)
		defer f.Close()

		for i:=0; i<count; i++ {
			key := fmt.Sprintf("key-%v", uuid.NewV4())
			val := rand.Int63n(72057594037927936)

			item := []interface{}{key, val}
			ch <- item

			f.WriteString(fmt.Sprintf("%v\t%v\n", key, val))
		}

		f.Sync()

		close(ch)
	}(q, logPath)

	return q
}


func TakeI64StrItems(logPath string) chan []interface{} {


	q := make(chan []interface{})

	go func(ch chan []interface{}, logPath string) {
		f, _ := os.OpenFile(logPath, os.O_RDWR, 0666)
		defer f.Close()

		fmt.Println("TakeItems", logPath)

		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			line := scanner.Text()
			parts := strings.Split(line, "\t")
			//fmt.Println("line", parts)
			key, _ := strconv.ParseInt(parts[0], 10, 64)
			item := []interface{}{int64(key), parts[1]}

			ch <- item

		}

		close(ch)

	} (q, logPath)

	return q
}

func TakeStrI64Items(logPath string) chan []interface{} {


	q := make(chan []interface{})

	go func(ch chan []interface{}, logPath string) {
		f, _ := os.OpenFile(logPath, os.O_RDWR, 0666)
		defer f.Close()

		fmt.Println("TakeItems", logPath)

		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			line := scanner.Text()
			parts := strings.Split(line, "\t")
			//fmt.Println("line", parts)
			value, _ := strconv.ParseInt(parts[1], 10, 64)
			item := []interface{}{parts[0], int64(value)}

			ch <- item

		}

		close(ch)

	} (q, logPath)

	return q
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
