package main

import (
	"os"
	"fmt"
	"time"
	//"bufio"
	//"strconv"
	//"strings"
	"math/rand"
	"github.com/satori/go.uuid"
	"../../gokvdb"
	"../testutils"
)


func main() {

	rand.Seed(time.Now().UTC().UnixNano())

	dbPath := "./testdata/lazy_stri64.kv"

	//total := 0
	//testCount := 4096
	/*
	counterCallback := func() int {
		total += 1
		return total
	}*/

	//logPath := "./testdata/lazy_i64str_test.items"

	
	logPath := testutils.CreateTempFilePath()

	for j:=0; j<10; j++ {
	
		for i:=0; i<10; i++ {
			//testHashDict(dbPath, "mydb", "get_int_by_str", testCount, counterCallback)
			//randItems := testutils.RandomStrI64Items(131072, logPath)
			randItems := testutils.RandomStrI64Items(131072, logPath)
			insertWithChan(dbPath, "mydb", "get_int_by_str", randItems)
			
		}

		validWithChan(dbPath, "mydb", "get_int_by_str", testutils.TakeStrI64Items(logPath))
	}

}

func validWithChan(dbPath string, dbName string, dictName string, items chan []interface{}) {

	testutils.OpenStorage(dbPath, func(s *gokvdb.Storage) {

			dict := gokvdb.NewStrI64Dict(s, dbName, dictName)

			counter := 0

			for item := range items {
				counter += 1
				k := item[0].(string)
				v := item[1].(int64)

				valResult, ok := dict.Get(k)

				isValid := v == valResult

				fmt.Printf("[%08d] GET key=%v value=%v ok=%v result=%v isValid=%v\n", counter, k, v, ok, valResult, isValid)
				if !isValid {
					fmt.Println("VALID ERROR!!")
					os.Exit(1)
				}

			}
	})
}

func insertWithChan(dbPath string, dbName string, dictName string, items chan []interface{}) {


	testutils.OpenStorage(dbPath, func(s *gokvdb.Storage) {

		dict := gokvdb.NewStrI64Dict(s, dbName, dictName)

		counter := 0
		saveCount := 0

		for item := range items {

			counter += 1
			key := item[0].(string)
			val := item[1].(int64)

			dict.Set(key, val)
			fmt.Println(fmt.Sprintf("%07d", counter), "SET", key, val)

			saveCount += 1

			if saveCount >= 16384 {
				saveCount = 0
				dict.Save()
			}
		}

		dict.Save()

	})


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

