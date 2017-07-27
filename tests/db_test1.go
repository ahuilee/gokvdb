package main

import (
	"os"
	"fmt"
	"time"
	"math/rand"
	"bytes"
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

	dbpath := "./testdata/testdb.kv"

	var i int

	for i=0; i<1024;i++ {
		TestBTree(dbpath, "mydb", "mybtree", 128)
	}

}

func TestBTree(dbPath string, dbName string, btName string, testCount int) {


	rand.Seed(time.Now().UTC().UnixNano())


	testData := make(map[int64][]byte)

	var startKey int64

	startKey = int64(rand.Int63n(4294967296))

	for i:=0; i<testCount; i++ {

		data := testutils.RandBytes(rand.Intn(32767) + 128)
		//key := rand.Uint64()//uint64(rand.Int63())
		key := int64(startKey + int64(i))
		testData[key] = data
		
	}

	testutils.OpenStorage(dbPath, func(s *gokvdb.Storage) {
		db, err := s.DB(dbName)

		if err != nil {
			fmt.Println("DB", db, "err", err)
			os.Exit(1)
		}

		bt, err := db.OpenBTree(btName)
		if err != nil {
			fmt.Println("bt", bt, "err", err)
			os.Exit(1)
		}
		for key, data1 := range testData {


				fmt.Println("SET", key, "bytes", len(data1))

				bt.Set(key, data1)

		}
		s.Save()

	})

	testutils.OpenStorage(dbPath, func(s *gokvdb.Storage) {
		db, err := s.DB(dbName)
		if err != nil {
			fmt.Println("DB", db, "err", err)
			os.Exit(1)
		}


		bt, err := db.OpenBTree(btName)
		if err != nil {
			fmt.Println("bt", bt, "err", err)
			os.Exit(1)
		}

		for key, data1 := range testData {

			data2, ok := bt.Get(key)
			if !ok {
				fmt.Println("NO KEY", key)
				os.Exit(1)

			}
			compareRtn := bytes.Compare(data1, data2)


			fmt.Printf("VALID key=%v bytes=%v compareRtn=%v\n", key,  len(data2), compareRtn)
			if compareRtn != 0 {
				fmt.Println("Valid Error!")
				os.Exit(1)
			}
		}
	})




}
