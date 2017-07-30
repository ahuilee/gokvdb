package main

import (
	"os"
	"fmt"
	"bytes"
	"time"
	"math/rand"
	"github.com/satori/go.uuid"
	"../../gokvdb"
	"../testutils"
)


func main() {

	rand.Seed(time.Now().UTC().UnixNano())

	dbPath := "./testdata/lazy_strblob.kv"

	total := 0
	testCount := 1024

	counterCallback := func() int {
		total += 1
		return total
	}

	for i:=0; i<128; i++ {
		testStrBloc(dbPath, "mydb", "get_blob_by_str", testCount, counterCallback)
	}


}

func testStrBloc(dbPath string, dbName string, dictName string, testCount int, counterCallback func() int ) {

	testData := make(map[string][]byte)

	testutils.OpenStorage(dbPath, func(s *gokvdb.Storage) {

		dict := gokvdb.NewStrBlobDict(s, dbName, dictName)

		for i:=0; i<testCount; i++ {
			key := fmt.Sprintf("key-%v", uuid.NewV4())
			val := testutils.RandBytes(rand.Intn(65535) + 256)
			testData[key] = val

			counter := counterCallback()

			fmt.Println(fmt.Sprintf("%07d", counter), "SET", key, "bytes", len(val))

			dict.Set(key, val)
		}
		dict.Save()
	})

	testutils.OpenStorage(dbPath, func(s *gokvdb.Storage) {

			dict := gokvdb.NewStrBlobDict(s, dbName, dictName)

			for k, v := range testData {

				valResult, ok := dict.Get(k)

				compareRtn := bytes.Compare(v, valResult)

				isValid := compareRtn == 0

				fmt.Printf("GET key=%v ok=%v bytes=%v isValid=%v\n", k, ok, len(valResult), isValid)
				if !isValid {
					fmt.Println("VALID ERROR!!")
					os.Exit(1)
				}


			}

	})

	testutils.OpenStorage(dbPath, func(s *gokvdb.Storage) {

		dict := gokvdb.NewStrBlobDict(s, dbName, dictName)

		counter := 0

		for item := range dict.Items() {
			counter += 1
			val := item.Value()
			fmt.Println("Items", fmt.Sprintf("%07d", counter), item.Key(), "bytes", len(val) )
		}
	})

}

