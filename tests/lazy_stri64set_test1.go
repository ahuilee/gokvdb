package main

import (
	//"os"
	"fmt"
	"time"
	"math/rand"
	"github.com/satori/go.uuid"
	"../../gokvdb"
	"../testutils"
)


func main() {

	rand.Seed(time.Now().UTC().UnixNano())

	dbPath := "./testdata/lazy_stri64set.kv"

	dbName := "mydb"
	dictName := "valset_by_key"
	
	for i:=0; i<100; i++{
		Test(dbPath, dbName, dictName)

	}

}

var testCounter = 0

func Test(dbPath string, dbName string, dictName string) {
	var keys []string

	testCounter += 1


	testutils.OpenStorage(dbPath, func(s *gokvdb.Storage) {

			set := gokvdb.NewStrI64SetDict(s, dbName, dictName)

			for i:= 0; i<32; i++ {
				key := fmt.Sprintf("key-%v", uuid.NewV4())
				vals := testutils.RandI64Array(1000)

				keys = append(keys, key)

				for _i, val := range vals {
					set.Add(key, val)
					fmt.Printf("%07d ADD SET i=%v k=%v v=%v\n", testCounter, _i, key, val) 
				}

			}

			

			set.Save()

	})

	testutils.OpenStorage(dbPath, func(s *gokvdb.Storage) {
		set := gokvdb.NewStrI64SetDict(s, dbName, dictName)

	
		for _, key := range keys {

			//fmt.Printf("GET key=%v...\n", key)
			item, ok := set.Get(key)
			if !ok {
				fmt.Printf("Valid error! no key=%v\n", key)
			}

			var vals []int64
			for val := range item.Values() {
				vals = append(vals, val)
			}


			fmt.Printf("%07d Get Item key=%v rows=%v\n",  testCounter, item.Key(), len(vals))
			/*
			for _, val := range vals {
				fmt.Printf("GET key=%v Value=%v\n", item.Key(), val)
			}*/
		}

	})


}
