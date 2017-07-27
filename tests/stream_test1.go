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

	fp := "./testdata/teststream.kv"

	rand.Seed(time.Now().UTC().UnixNano())


	for i:=0; i<65535; i++ {
		data := testutils.RandBytes(rand.Intn(1024)+1024)

		stream, _ := gokvdb.OpenFileStream(fp)

		seek2 := int64(rand.Intn(536870912))

		fmt.Println("Write", stream, "seek2", seek2, "bytes", len(data))
		stream.Seek(seek2)
		stream.Write(data)
		stream.Close()

		stream, _ = gokvdb.OpenFileStream(fp)

		stream.Seek(seek2)
		data2, err := stream.Read(len(data))

		compareRtn := bytes.Compare(data, data2)
		fmt.Println("Read Valid", i, "err", err, "compare", compareRtn)

		if compareRtn != 0 {
			fmt.Println("Valid Error!")
			os.Exit(1)
		}
	}



}
