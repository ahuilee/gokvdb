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

var insertCount = 0

func main() {

	rand.Seed(time.Now().UTC().UnixNano())

	dbPath := fmt.Sprintf("./testdata/lazy_i64_str_%v.kv", time.Now().UTC().UnixNano())
	dbName := "mydb"
	dictName := "get_str_by_int"

	logPath := testutils.CreateTempFilePath()

	for j:=0; j<8; j++ {

		for i:=0; i<16; i++ {
			//startKey := rand.Int63n(72057594037927936)
			randItems := testutils.RandomI64StrItems(10000, logPath)
			insertI64StrDictWithChan(dbPath, dbName, dictName, randItems)
		}

		validI64StrDictWithChan(dbPath, dbName, dictName, testutils.TakeI64StrItems(logPath))
		
	}


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
			insertCount += 1
			key := item[0].(int64)
			val := item[1].(string)
			fmt.Printf("%08d SET key=%v val=%v\n", insertCount, key, val)
			dict.Set(key, val)
		}
		dict.Save(true)
	})



}

