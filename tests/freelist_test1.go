package main

import (
	//"os"
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

	dbPath := "./testdata/freelist_test.kv"
	pageSize := 4096
	metaOffset := 0

	for i:=0; i<4096; i++ {

		var pid uint32
		testutils.OpenStreamPager(dbPath, pageSize, metaOffset, "w", func(pager gokvdb.IPager) {
			pid = pager.CreatePageId()
		})

		for j:=0; j<8; j++ {

			testutils.OpenStreamPager(dbPath, pageSize, metaOffset, "w", func(pager gokvdb.IPager) {

				data1 := testutils.RandBytes(rand.Intn(131072) + 65536)

				fmt.Println("Write", len(data1))

				pager.WritePayloadData(pid, data1)

				
				data2 := testutils.RandBytes(rand.Intn(4096) + 1)
				fmt.Println("Write2", len(data2))

				pager.WritePayloadData(pid, data2)

				//fmt.Println(

			})
		}

	}


}

