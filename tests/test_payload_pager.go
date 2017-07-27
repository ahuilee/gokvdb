package main

import (
	"os"
	"fmt"
	"time"
	"bytes"
	"math/rand"
	//"github.com/satori/go.uuid"	
	"../../gokvdb"
	"../testutils"
)

func CheckErr(err error) {
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func main() {

	dbPath := "./testdata/teststorage.kv"
	dbName := "mydb"


	var i int

	for i=0; i<100;i++ {
		TestPayloadPager(dbPath, dbName, "bytree")
	}
}


func TestPayloadPager(dbPath string, dbName string, ixName string) {

	rand.Seed(time.Now().UTC().UnixNano())

	testData := make(map[uint64][]byte)

	var startKey uint64

	startKey = uint64(rand.Int63n(4294967296))

	for i:=0; i<512; i++ {

		data := testutils.RandBytes(rand.Intn(32767) + 32767)
		//key := rand.Uint64()//uint64(rand.Int63())
		key := uint64(startKey + uint64(i))
		testData[key] = data
		
	}

	testutils.OpenStorage(dbPath, func(s *gokvdb.Storage) {

		db, err := s.DB(dbName)
		CheckErr(err)

		pager := db.GetPayloadPager()

		for key, data := range testData {
			fmt.Println("INSERT", key, "bytes", len(data))
			pager.Set(key, data)
		}

		s.Save()
	})

	testutils.OpenStorage(dbPath, func(s *gokvdb.Storage) {

		db, err := s.DB(dbName)
		CheckErr(err)

		pager := db.GetPayloadPager()
		

		for key, data := range testData {
			t1 := time.Now()
			data2, ok := pager.Get(key)
			dt := time.Since(t1)
			if !ok {
				fmt.Println("Valid NO KEY ERROR", key)
				os.Exit(1)
			}
			cpResult := bytes.Compare(data, data2)
			fmt.Println("Valid BTree", key, "compare", cpResult, "bytes", len(data2), "dt", dt)

			if cpResult != 0 {
				fmt.Println("Valid Error!")
				os.Exit(1)
			}
		}
	})
	

	testutils.OpenStorage(dbPath, func(s *gokvdb.Storage) {

		db, err := s.DB(dbName)
		CheckErr(err)
		
		pager := db.GetPayloadPager()

		t1 := time.Now()
		count := 0

		for range pager.Items() {
			//fmt.Println("BTreeTestItems", item.Key(), "bytes", len(item.Value()))
			count += 1
		}
		dt := time.Since(t1)

		fmt.Println("ForEach", count, dt)
	})

	fmt.Println("startKey", startKey)

}


