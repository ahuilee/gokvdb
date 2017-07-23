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

	dbpath := "./testdata/test.db"

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

	t1 := time.Now()

	db := gokvdb.OpenHash(dbpath)
	
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

}

func _TestInsert(dbpath string, count int) {
	db := gokvdb.OpenHash(dbpath)
	for i:=0; i<count; i++ {

		key := fmt.Sprintf("%v", uuid.NewV4())
		val := _RandBytes(int(rand.Intn(200) + 1))
		fmt.Println("SET", key, "bytes", len(val))

		db.Set(key, val)

	}
	db.Close()
}

func _TestUpdate(dbpath string, count int) {

	setCount := 0
	getCount := 0
	
	for i:=0; i<count ;i++ {
		db := gokvdb.OpenHash(dbpath)

		dict := make(map[string][]byte)

		for j:=0; j<1024; j++ {

			//key := fmt.Sprintf("hello-%v", rand.Int31() + 1)
			key := fmt.Sprintf("%v", uuid.NewV4())
			size := int(rand.Intn(200) + 1)
			val := make([]byte, size)
			for j:=0; j<size; j++ {
				val[j] = byte(rand.Intn(255))
			}
			dict[key]= val

			setCount += 1

			fmt.Println(fmt.Sprintf("%07d", setCount), "Set", key, "len..", len(val))
		}		

		db.Update(dict)
		db.Close()

		db = gokvdb.OpenHash(dbpath)

		for k, v := range dict {
			val2, ok := db.Get(k)

			compare :=  bytes.Compare(v, val2)
			getCount += 1
			fmt.Println(fmt.Sprintf("%07d", getCount), "Get", k, ok, "compare", compare)

			if compare != 0 {
				fmt.Println("Value valid error", v, val2)
				os.Exit(1)
			}
		}
		db.Close()
	}

}

func _RandBytes(size int) []byte {
	output := make([]byte, size)
	for j:=0; j<size; j++ {
		output[j] = byte(rand.Intn(255))
	}
	return output
}
