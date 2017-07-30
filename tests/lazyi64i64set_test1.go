

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
	
	dbPath := "./testdata/lazy_i64i64_set.kv"
	dbName := "mydb"
	dictName := "i64i64_set"


	testutils.OpenStorage(dbPath, func(s *gokvdb.Storage) {

			set := gokvdb.NewI64I64Set(s, dbName, dictName)


			key := int64(2)

			for i:=0; i<32; i++ {
				val := rand.Int63n(4294967296)
				set.Add(key, val)
				fmt.Printf("SET key=%v value=%v\n", key, val)
			}

			set.Save()

	})

}


