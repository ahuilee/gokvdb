package main


import (
	"os"
	"fmt"
	//"bytes"
	"time"
	"math/rand"
	"../../gokvdb"
	"../testutils"
)


func main() {

	rand.Seed(time.Now().UTC().UnixNano())

	dbPath := "./testdata/test_treepage.kv"
	pageSize := 4096


	for i:=0; i<1; i++ {
		TestTree(dbPath, pageSize, 128)
	}


}

func TestTree(dbPath string, pageSize int, testCount int) {



	var pid uint32
	metaOffset := 0

	testData := make(map[int64]int64)

	testutils.OpenStreamPager(dbPath, pageSize, metaOffset, "w", func(pager gokvdb.IPager) {

		pid = pager.CreatePageId()
		tree := gokvdb.NewI64I64BTreePage(nil)

		for i:=0; i<4096; i++ {
			k := rand.Int63n(68719476736)
			v := rand.Int63n(68719476736)
			tree.Insert(k, v)
			fmt.Println("Insert", k, v)

			testData[k] = v
		}

		pager.WritePayloadData(pid, tree.ToBytes())

	})


	testutils.OpenStreamPager(dbPath, pageSize, metaOffset, "r", func(pager gokvdb.IPager) {

		data, _ := pager.ReadPayloadData(pid)

		tree := gokvdb.NewI64I64BTreePage(data)

		for k, v := range testData {
			v2, ok := tree.Get(k)
			if !ok {
				fmt.Println("GET NoKey", k)
				os.Exit(1)
			}
			fmt.Printf("GET k=%v v=%v v2=%v\n", k, v, v2)
			if v != v2 {
				os.Exit(1)
			}
		}

	})


	testutils.OpenStreamPager(dbPath, pageSize, metaOffset, "r", func(pager gokvdb.IPager) {

		data, _ := pager.ReadPayloadData(pid)

		tree := gokvdb.NewI64I64BTreePage(data)

		fmt.Println("Load", tree.Count())

		count := 0

		for range tree.Items() {
			count += 1
			//fmt.Printf("Items %07d k=%v v=%v\n", count, item.Key(), item.Value())
		}
		fmt.Printf("Items %07d\n", count)


	})


}




