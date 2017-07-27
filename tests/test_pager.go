package main

import (
	"os"
	"fmt"
	"bytes"
	"math/rand"
	"../../gokvdb"
)


func main() {


	openCall := func(callback func(*gokvdb.FilePager)) {
		pager, hdrBytes, err := gokvdb.OpenFilePager("./testdata/testpager.kv", 4096)
		if err != err {
			fmt.Println("err", err)
			os.Exit(1)
		}
		callback(pager)
		pager.Close()

	}

	for _i:=0; _i<4096; _i++ {

		var pid uint32
		data := make([]byte, 10000)
		for i:=0; i<len(data); i++ {
			data[i] = byte(rand.Intn(255))
		}

		openCall(func(pager *gokvdb.FilePager) {
			pid = pager.CreatePageId()
			fmt.Println("CreatePageId", pid)
			pager.SavePayloadData(pid, data)
			pager.Save()

		})

		openCall(func(pager *gokvdb.FilePager) {
			
			data2, err := pager.LoadPayloadData(pid)
			compareResult := bytes.Compare(data, data2)
			fmt.Println("err", err, "compareResult", compareResult)

		})
	}


}
