package main


import (
	"os"
	"fmt"
	"bytes"
	"time"
	"math/rand"
	"../../gokvdb"
	"../testutils"
)



func main() {

	for i:=0; i<32767; i++ {
		TestPayload()
	}


}

func TestPayload() {

	rand.Seed(time.Now().UTC().UnixNano())

	fp := "./testdata/test_payload.kn"
	pageSize := 4096
	metaOffset := 0

	testData := make(map[uint32][]byte)


	for i:=0; i<16; i++ {

		var pids []uint32

		for j :=0; j<16; j++ {
		

			testutils.OpenStreamPager(fp, pageSize, metaOffset, "w", func(pager gokvdb.IPager) {
				pid := pager.CreatePageId()
				pids = append(pids, pid)
				
				fmt.Println("CreatePageId", pid)
			})
		}

		for j:=0; j<16; j++ {

			for _, pid := range pids {

				data := testutils.RandBytes(rand.Intn(65535) + 1)
				testutils.OpenStreamPager(fp, pageSize, metaOffset, "w", func(pager gokvdb.IPager) {
					//data := testutils.RandBytes(8000)
					pager.WritePayloadData(pid, data)
					testData[pid] = data		

				})

				testutils.OpenStreamPager(fp, pageSize, metaOffset, "r", func(pager gokvdb.IPager) {
				
					//data := testutils.RandBytes(8000)
					data2, _ := pager.ReadPayloadData(pid)

					compareRtn := bytes.Compare(data, data2)

					fmt.Println("............pid=", pid, "compareRtn", compareRtn)
					if compareRtn != 0 {
						os.Exit(1)
					}
					

				})
			}
		}

	}


	testutils.OpenStreamPager(fp, pageSize, metaOffset, "r", func(pager gokvdb.IPager) {

		for pid, data := range testData {
			//data := testutils.RandBytes(8000)
			data2, err := pager.ReadPayloadData(pid)

			compareRtn := bytes.Compare(data, data2)

			fmt.Println("ReadPayloadData pid=", pid, "err", err, "bytes", len(data), "compare", compareRtn)
			if compareRtn != 0 {
				fmt.Println("Valid Error!")
				os.Exit(1)
			}
		}
	})


}
