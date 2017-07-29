package main


import (
	//"os"
	"fmt"
	"bytes"
	"time"
	"strings"
	"math/rand"
	"../../gokvdb"
	"../testutils"
)


func main() {

	rand.Seed(time.Now().UTC().UnixNano())


	dbPath := "./testdata/testinternalpager.kv"


	for i:=0; i<4096; i++ {
		TestInternalPager_Page(dbPath)
		TestInternalPager_Payload(dbPath)
	}

}

func TestInternalPager_Payload(dbPath string) {

	testData := make(map[uint32][]byte)


	for i:=0; i<32; i++ {

		var pid uint32
		var pageData1 []byte


		testutils.OpenInternalPager(dbPath, 4096, 0, "w", func(pager gokvdb.IPager) {

			pid = pager.CreatePageId()
		})

		for j:=0; j<1; j++ {

			testutils.OpenInternalPager(dbPath, 4096, 0, "w", func(pager gokvdb.IPager) {
				fmt.Println(strings.Repeat("-", 50))

				pageData1 = testutils.RandBytes(rand.Intn(65535) + 65535)

				testData[pid] = pageData1

				fmt.Println("[TestInternalPager] WritePayloadData Large pid", pid, "bytes", len(pageData1))
				pager.WritePayloadData(pid, pageData1)

			})

			testutils.OpenInternalPager(dbPath, 4096, 0, "w", func(pager gokvdb.IPager) {

				fmt.Println(strings.Repeat("-", 50))

				pageData2 := testutils.RandBytes(rand.Intn(1024) + 1)

				testData[pid] = pageData2

				fmt.Println("[TestInternalPager] WritePayloadData Small pid", pid, "bytes", len(pageData2))
				pager.WritePayloadData(pid, pageData2)

			})

			testutils.OpenInternalPager(dbPath, 4096, 0, "r", func(pager gokvdb.IPager) {

				pageData2, err := pager.ReadPayloadData(pid)

				compareRtn := bytes.Compare(pageData1, pageData2)

				fmt.Println("[TestInternalPager] ReadPage pid", pid, "bytes1", len(pageData1), "err", err, "bytes2", len(pageData2), "compareRtn", compareRtn)

			})
		}
		
	}


}

func TestInternalPager_Page(dbPath string) {

	testData := make(map[uint32][]byte)

	for i:=0; i<32; i++ {

		var pid uint32
		testutils.OpenInternalPager(dbPath, 4096, 0, "w", func(pager gokvdb.IPager) {
				pid = pager.CreatePageId()
		})

		for j:=0; j<1; j++ {

			testutils.OpenInternalPager(dbPath, 4096, 0, "w", func(pager gokvdb.IPager) {

					pageData1 := testutils.RandBytes(rand.Intn(128) + 256)

					testData[pid] = pageData1

					pager.WritePage(pid, pageData1)

					fmt.Println("[TestInternalPager] WritePage pid", pid, "bytes", len(pageData1))
				
			})
		}
	}

	testutils.OpenInternalPager(dbPath, 4096, 0, "r", func(pager gokvdb.IPager) {

		for pid, pageData1 := range testData {
			pageData2, ok := pager.ReadPage(pid, 0)
			
			compareRtn := bytes.Compare(pageData1, pageData2)

			fmt.Println("[TestInternalPager] ReadPage pid", pid, "bytes1", len(pageData1), "read ok", ok, "bytes2", len(pageData2), "compareRtn", compareRtn)


		}
	})


}
