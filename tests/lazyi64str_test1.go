package main

import (
	"os"
	"fmt"
	//"bytes"
	"time"
	"math/rand"
	"github.com/satori/go.uuid"
	"../../gokvdb"
	"../testutils"
)


func main() {

	rand.Seed(time.Now().UTC().UnixNano())

	dbPath := "./testdata/lazy_i64_str.kv"

	testCount := 131072
	startKey := int64(1)

	total := 0


	for i:=0; i<128; i++ {
		//startKey := rand.Int63n(72057594037927936)
		testI64StrDict(dbPath, "mydb", "get_str_by_int", startKey, testCount)
		total += testCount
		startKey += int64(testCount)
	}

	fmt.Println("Test total", total)


}

func testI64StrDict(dbPath string, dbName string, dictName string, startKey int64, testCount int ) {

	testData := make(map[int64]string)	

	for i:=0; i<testCount; i++ {
			key := startKey + int64(i)
			val := fmt.Sprintf("val-%v", uuid.NewV4())
			testData[key] = val
	}
	testI64StrDictWithDict(dbPath, dbName, dictName, testData)

}


func testI64StrDictWithDict(dbPath string, dbName string, dictName string, testData map[int64]string) {
	

	testutils.OpenStorage(dbPath, func(s *gokvdb.Storage) {

		dict := gokvdb.NewI64StrDict(s, dbName, dictName)

		for key, val := range testData {
			fmt.Println("SET", key, val)
			dict.Set(key, val)
		}
		dict.Save()
	})

	testutils.OpenStorage(dbPath, func(s *gokvdb.Storage) {

			dict := gokvdb.NewI64StrDict(s, dbName, dictName)

			for k, v := range testData {

				valResult, ok := dict.Get(k)

				isValid := v ==valResult

				fmt.Printf("GET key=%v ok=%v result=%v isValid=%v\n", k, ok, valResult, isValid)
				if !isValid {
					fmt.Println("VALID ERROR!!")
					os.Exit(1)
				}


			}

	})

	testutils.OpenStorage(dbPath, func(s *gokvdb.Storage) {

			dict := gokvdb.NewI64StrDict(s, dbName, dictName)

			counter := 0

			for item := range dict.Items() {
				counter += 1
				fmt.Println("Items", fmt.Sprintf("%07d", counter), item.Key(), item.Value())
			}

	})

}

