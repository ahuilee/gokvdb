package main

import (
	"os"
	"fmt"
	//"bytes"
	"time"
	"math/rand"
	"sort"
	//"github.com/satori/go.uuid"
	"../../gokvdb"
	"../testutils"
)


func main() {

	rand.Seed(time.Now().UTC().UnixNano())

	dbPath := "./testdata/lazy_i64i64set.kv"
	dbName := "mydb"
	dictName := "get_i64_i64set"

	//testCount := 131072
	

	//total := 0



	for j:=0; j<128; j++ {
		testInsert(dbPath, dbName, dictName)

		//validI64StrDictWithChan(dbPath, dbName, dictName, testutils.TakeI64StrItems(logPath))

		
	}

	//fmt.Println("Test total", total)


} 

var testCounter = 0
type I64Array []int64

func (self I64Array) Len() int { return len(self) }
func (self I64Array) Swap(i, j int) { self[i], self[j] = self[j], self[i] }
func (self I64Array) Less(i, j int) bool { return self[i] < self[j] }


func testInsert(dbPath string, dbName string, dictName string) {

	testCounter += 1


	checkMap := make(map[int64]map[int64]byte)


	for i:= 0; i<128; i++ {


		key := rand.Int63n(12345678)

		checkData := make(map[int64]byte)
		checkMap[key] = checkData

		counter := 0


		for j:= 0; j<32; j++ {
		
			testutils.OpenStorage(dbPath, func(s *gokvdb.Storage) {

				i64i64set := gokvdb.NewLazyI64I64SetDict(s, dbName, dictName)				

				vals := testutils.RandI64Array(100)
				
				for _, v := range vals {
					//for i:=0; i<16384; i++{
					counter += 1
					i64i64set.Add(key, v)
					fmt.Printf("%04d Add count=%06d key=%v val=%v\n", testCounter, counter, key, v)

					checkData[v] = 0
				}

				i64i64set.Save(true)

			})
		}

	}


	testutils.OpenStorage(dbPath, func(s *gokvdb.Storage) {
		i64i64set := gokvdb.NewLazyI64I64SetDict(s, dbName, dictName)
		for key, checkData := range checkMap {
				item, ok := i64i64set.Get(key)
				if !ok {
					fmt.Println("No key", key)

					os.Exit(1)
				}

				var vals I64Array
				for v, _ := range checkData {
					vals = append(vals, v)
				}

				

				sort.Sort(vals)
				i:= 0

				for setVal := range item.Values() {
					val := vals[i]
					isValid := val == setVal

					fmt.Printf("%04d Get key=%v setVal=%v val=%v isValid=%v\n", testCounter, key, setVal, val, isValid)
					if !isValid {
						os.Exit(1)
					}

					i += 1

				}
				
		}
	})


	



}

