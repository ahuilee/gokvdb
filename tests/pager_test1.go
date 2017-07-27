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

	rand.Seed(time.Now().UTC().UnixNano())

	fp := "./testdata/test_pager.kv"
	pageSize := 4096	

	for i:=0; i<65535; i++ {
		data := testutils.RandBytes(rand.Intn(1024)+1024)

		var pid uint32

		testutils.OpenStreamPager(fp, pageSize, func(pager gokvdb.IPager) {

			fmt.Println("OpenStreamPager", pager.ToString())

			pid = pager.CreatePageId()
			pager.WritePage(pid, data)

			fmt.Printf("WritePage pid=%v bytes=%v\n", pid, len(data))

			pager.Save()

		})

		testutils.OpenStreamPager(fp, pageSize, func(pager gokvdb.IPager) {		

			data2, err := pager.ReadPage(pid)
			compareRtn := bytes.Compare(data, data2[:len(data)])
			fmt.Println("Read Valid", i, "err", err, "data2", len(data2), "compare", compareRtn)
			

			if compareRtn != 0 {
				fmt.Println("Valid Error!")
				os.Exit(1)
			}

		})

	}


}

