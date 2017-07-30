package main

import (
	"os"
	"fmt"
	//"bytes"
	"time"
	"math/rand"
	//"github.com/satori/go.uuid"
	"../../gokvdb"
	"../testutils"
)


func main() {

	rand.Seed(time.Now().UTC().UnixNano())

	dbPath := "./testdata/lazy_i64i64set.kv"
	dbName := "mydb"
	dictName := "get_i64_i64set"

	//testCount := 131072
	

	//total := 0



	for j:=0; j<128; j++ {
		testInsert(dbPath, dbName, dictName)

		//validI64StrDictWithChan(dbPath, dbName, dictName, testutils.TakeI64StrItems(logPath))

		
	}

	//fmt.Println("Test total", total)


} 

var testCounter = 0



func testInsert(dbPath string, dbName string, dictName string) {

	testCounter += 1


	for i:= 0; i<128; i++ {

		var keys []int64

		for j:= 0; j<128; j++ {
		
			testutils.OpenStorage(dbPath, func(s *gokvdb.Storage) {

				i64i64set := gokvdb.NewLazyI64I64SetDict(s, dbName, dictName)

				key := rand.Int63n(12345678)

				keys = append(keys, key)

				vals := testutils.RandI64Array(100)
				for _i, v := range vals {
					i64i64set.Add(key, v)
					fmt.Printf("%04d Add j=%04d i=%06d key=%v val=%v\n", testCounter, j, _i, key, v)
				}

				i64i64set.Save()

			})
		}


		testutils.OpenStorage(dbPath, func(s *gokvdb.Storage) {

			i64i64set := gokvdb.NewLazyI64I64SetDict(s, dbName, dictName)

			count := 0

			for item := range i64i64set.Items() {
				key := item.Key()
				for val := range item.Values() {
					count += 1
					fmt.Printf("%04d Items i=%04d %07d key=%v val=%v\n", testCounter, i, count, key, val)
				}
			}

			for _, key := range keys {
				item, ok := i64i64set.Get(key)


				if !ok {
					fmt.Printf("%04d Get Error! key=%vv\n", testCounter, key)
					os.Exit(1)
				}
				isValid := key == item.Key()
				fmt.Printf("%04d Get key=%v isValid=%v rows=%v\n", testCounter, key, isValid, len(item.Values()))

			}

		})

	}


	



}

