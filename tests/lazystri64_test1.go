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

	dbPath := "./testdata/lazy_i64str.kv"

	total := 0
	testCount := 65535

	counterCallback := func() int {
		total += 1
		return total
	}



	for i:=0; i<1024; i++ {
		testHashDict(dbPath, "mydb", "get_int_by_str", testCount, counterCallback)
	}


}

func testHashDict(dbPath string, dbName string, dictName string, testCount int, counterCallback func() int ) {

	testData := make(map[string]int64)

	testutils.OpenStorage(dbPath, func(s *gokvdb.Storage) {

		dict := gokvdb.NewStrI64Dict(s, dbName, dictName)

		for i:=0; i<testCount; i++ {
			key := fmt.Sprintf("key-%v", uuid.NewV4())
			val := rand.Int63n(72057594037927936)
			testData[key] = val

			counter := counterCallback()

			fmt.Println(fmt.Sprintf("%07d", counter), "SET", key, val)

			dict.Set(key, val)
		}
		dict.Save()
	})

	testutils.OpenStorage(dbPath, func(s *gokvdb.Storage) {

			dict := gokvdb.NewStrI64Dict(s, dbName, dictName)

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

		dict := gokvdb.NewStrI64Dict(s, dbName, dictName)

		counter := 0

		for item := range dict.Items() {
			counter += 1
			fmt.Println("Items", fmt.Sprintf("%07d", counter), item.Key(), item.Value())
		}
	})

}

