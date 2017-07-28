package main

import (
	"os"
	"fmt"
	"bytes"
	"time"
	"math/rand"
	//"github.com/satori/go.uuid"
	"../../gokvdb"
	"../testutils"
)


func main() {

	rand.Seed(time.Now().UTC().UnixNano())

	dbPath := "./testdata/lazy_i64_blob.kv"

	testCount := 1024
	startKey := int64(1)

	total := 0


	for i:=0; i<128; i++ {
		//startKey := rand.Int63n(72057594037927936)
		testI64BlocDict(dbPath, "mydb", "get_str_by_int", startKey, testCount)
		total += testCount
		startKey += int64(testCount)
	}

	fmt.Println("Test total", total)


}

func testI64BlocDict(dbPath string, dbName string, dictName string, startKey int64, testCount int ) {

	testData := make(map[int64][]byte)	

	for i:=0; i<testCount; i++ {
			key := startKey + int64(i)
			val := testutils.RandBytes(rand.Intn(16384) + 256)
			testData[key] = val
	}
	testI64BlobDictWithDict(dbPath, dbName, dictName, testData)

}


func testI64BlobDictWithDict(dbPath string, dbName string, dictName string, testData map[int64][]byte) {
	

	testutils.OpenStorage(dbPath, func(s *gokvdb.Storage) {

		dict := gokvdb.NewI64BlobDict(s, dbName, dictName)

		for key, val := range testData {
			fmt.Println("SET", key, "bytes", len(val))
			dict.Set(key, val)
		}
		dict.Save()
	})

	testutils.OpenStorage(dbPath, func(s *gokvdb.Storage) {

			dict := gokvdb.NewI64BlobDict(s, dbName, dictName)

			for k, v := range testData {

				valResult, ok := dict.Get(k)

				compareRtn := bytes.Compare(v, valResult)

				isValid := compareRtn == 0

				fmt.Printf("GET key=%v ok=%v result=%v bytes=%v isValid=%v\n", k, ok, len(valResult), isValid)
				if !isValid {
					fmt.Println("VALID ERROR!!")
					os.Exit(1)
				}


			}

	})

	testutils.OpenStorage(dbPath, func(s *gokvdb.Storage) {

			dict := gokvdb.NewI64BlobDict(s, dbName, dictName)

			counter := 0

			for item := range dict.Items() {
				counter += 1
				fmt.Println("Items", fmt.Sprintf("%07d", counter), item.Key(), "bytes", len(item.Value()))
			}

	})

}

