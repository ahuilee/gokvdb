package main


import (
	//"os"
	"fmt"
	"bytes"
	"time"
	"math/rand"
	"../../gokvdb"
	"../testutils"
)


func main() {

	rand.Seed(time.Now().UTC().UnixNano())

	dbPath := "./testdata/test_tree.kv"
	pageSize := 4096


	for i:=0; i<4096; i++ {
		TestTree(dbPath, pageSize, 128)
	}


}

func TestTree(dbPath string, pageSize int, testCount int) {




	testData := make(map[int64][]byte)

	for i:=0; i<testCount; i++ {

		var key int64
		var data1 []byte

		key = rand.Int63n(8589934592)

		testutils.OpenBTeeBlobMap(dbPath, pageSize, "w", func(tree *gokvdb.BTreeBlobMap) {

			data1 = testutils.RandBytes(rand.Intn(16384) + 512)

			tree.Set(key, data1)
			testData[key] = data1

			fmt.Printf("SET key=%v compareRtn=%v bytes=%v \n", key , len(data1))

		})

	}

	testutils.OpenBTeeBlobMap(dbPath, pageSize, "r", func(tree *gokvdb.BTreeBlobMap) {

		for key, data1 := range testData {

			data2 , ok := tree.Get(key)
			if !ok {
				fmt.Printf("VALID key=%v Failed!\n", key)
			}
			compareRtn := bytes.Compare(data1, data2)
			fmt.Printf("VALID key=%v bytes=%v compareRtn=%v\n", key , len(data2), compareRtn)
		}

	})



}




