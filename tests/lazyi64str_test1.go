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

	dbPath := "./testdata/lazy_i64_str.kv"
	dbName := "mydb"
	dictName := "get_str_by_int"

	testCount := 131072
	

	total := 0

	logPath := testutils.CreateTempFilePath()

	for j:=0; j<10; j++ {

		for i:=0; i<10; i++ {
			//startKey := rand.Int63n(72057594037927936)
			randItems := testutils.RandomI64StrItems(131072, logPath)
			insertI64StrDictWithChan(dbPath, dbName, dictName, randItems)
			total += testCount

		}

		validI64StrDictWithChan(dbPath, dbName, dictName, testutils.TakeI64StrItems(logPath))

		
	}

	fmt.Println("Test total", total)


}

 
func validI64StrDictWithChan(dbPath string, dbName string, dictName string, items chan []interface{}) {

		testutils.OpenStorage(dbPath, func(s *gokvdb.Storage) {

			dict := gokvdb.NewI64StrDict(s, dbName, dictName)

			counter := 0

			for item := range items {

				counter += 1

				key := item[0].(int64)
				val := item[1].(string)

				valResult, ok := dict.Get(key)

				isValid := val ==valResult

				fmt.Printf("[%08d] GET key=%v ok=%v result=%v isValid=%v\n", counter, key, ok, valResult, isValid)
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
				fmt.Println("Items", fmt.Sprintf("%08d", counter), item.Key(), item.Value())
			}

	})


}


func insertI64StrDictWithChan(dbPath string, dbName string, dictName string, items chan []interface{}) {
	

	testutils.OpenStorage(dbPath, func(s *gokvdb.Storage) {

		dict := gokvdb.NewI64StrDict(s, dbName, dictName)

		for item := range items {
			key := item[0].(int64)
			val := item[1].(string)
			fmt.Println("SET", key, val)
			dict.Set(key, val)
		}
		dict.Save()
	})



}

