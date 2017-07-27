package main

import (
	"os"
	"fmt"
	//"time"
	//"bytes"
	//"math/rand"
	"../../gokvdb"
	//"github.com/satori/go.uuid"	
)


func main() {

	var i16 uint16
	var i32 uint32

	maxUInt16 := uint16(65535)
	maxUInt32 := uint32(4294967294)

	for i16=0; i16<maxUInt16; i16++ {
		w := gokvdb.NewDataStream()
		w.WriteUInt16(i16)
		
		data := w.ToBytes()

		rd := gokvdb.NewDataStreamFromBuffer(data)
		rtn := rd.ReadUInt16()
		fmt.Println("TEST WriteUInt16", i16, rtn)
		if rtn != i16{
			fmt.Println("ERROR")
			os.Exit(1)
		}
	}

	for i32=0; i32<maxUInt32; i32++ {
		w := gokvdb.NewDataStream()
		w.WriteUInt32(i32)
		
		data := w.ToBytes()

		rd := gokvdb.NewDataStreamFromBuffer(data)
		rtn := rd.ReadUInt32()
		fmt.Println("TEST WriteUInt32", i32, rtn)
		if rtn != i32{
			fmt.Println("ERROR")
			os.Exit(1)
		}
	}


}
