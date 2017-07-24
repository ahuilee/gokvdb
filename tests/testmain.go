package main

import (	
	"os"	

	"math/rand"
	"bytes"
	"fmt"
	"time"
	"strconv"
	"strings"
	"github.com/satori/go.uuid"	
	"../../gokvdb"
)


func main() {

	args := os.Args[1:]

	testName := args[0]

	dbpath := "./testdata/test.kv"

	switch strings.ToUpper(testName) {
	case "INSERT":
		count, err := strconv.Atoi(args[1])
		if err != nil {
			count = 1
		}
		_TestInsert(dbpath, count)
	case "UPDATE":
		count, err := strconv.Atoi(args[1])
		if err != nil {
			count = 1
		}
		_TestUpdate(dbpath, count)

	case "ITEMS":
		_TestItems(dbpath, true)

	}
}

func _TestItems(dbpath string, isPrint bool) {



	_Open(dbpath, func(db *gokvdb.DB) {

	t1 := time.Now()
			i := 0

		for item := range db.Items() {
			if isPrint {
				fmt.Println("Items", i, "key", item.Key(), "bytes", len(item.Value()))
			}
			i += 1
		}
		fmt.Println("ItemsTotal", i)

		dt := time.Since(t1)
		fmt.Println("dt", dt)

	})

}

func _Open(dbpath string, callback func(db *gokvdb.DB)) {
	db, err := gokvdb.OpenHash(dbpath)
	if err == nil {
		callback(db)
		db.Close()
	}
}


func _TestInsert(dbpath string, count int) {

	data := _RandData(count)

	_Open(dbpath, func(db *gokvdb.DB) {

		for k, v := range data {
			t1 := time.Now()
			db.Set(k, v)
			dt := time.Since(t1)
			fmt.Println("SET", k, "bytes", len(v), "dt", dt)
		}
	})

	_ValidData(dbpath, data)

}

func _ValidData(dbpath string, data map[string][]byte) {

	fmt.Println(strings.Repeat("-", 100))

	fmt.Println(">> ValidData")

	_Open(dbpath, func(db *gokvdb.DB) {

		validCount := 0

		for k, v := range data {
			val2, _ := db.Get(k)
			compare :=  bytes.Compare(v, val2)
			validCount += 1
			//fmt.Println(fmt.Sprintf("%07d", getCount), "VALID", k, "compare", compare)

			if compare != 0 {
				fmt.Println("Value valid error", len(v), len(val2))
				os.Exit(1)
			}
		}

		fmt.Println("VALID SUCCESS count", validCount)
	})
}

func _TestUpdate(dbpath string, count int) {


	
	for i:=0; i<count ;i++ {

		dict := _RandData(1024)

		fmt.Println("Update", len(dict))
		_Open(dbpath, func(db *gokvdb.DB) {			
			db.Update(dict)			
		})


		_ValidData(dbpath, dict)

	}
}

func _RandBytes(size int) []byte {
	output := make([]byte, size)
	for j:=0; j<size; j++ {
		output[j] = byte(rand.Intn(255))
	}
	return output
}

func _RandData(count int) map[string][]byte {

	data := make(map[string][]byte)
	for i:=0; i<count; i++ {
			key := fmt.Sprintf("%v", uuid.NewV4())
			val := _RandBytes(int(rand.Intn(8192) + 8192))
			data[key] = val
	}

	return data
}

