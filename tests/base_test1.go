package main

import (
	//"os"
	"fmt"
	//"bytes"
	//"sort"
	//"time"
	//"math/rand"
	"../../gokvdb"
	//"../testutils"
)

func main() {

	list := gokvdb.NewLazyList()

	for i:=0; i<4096; i++ {
		list.Append(i)
	}

	for {
		if list.Len() == 0 {
			break
		}
		fmt.Println("Pop", list.Pop())
	}

}
