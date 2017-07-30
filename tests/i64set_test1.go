package main


import (
	"os"
	"fmt"
	//"bytes"
	"sort"
	"time"
	"math/rand"
	"../../gokvdb"
	"../testutils"
)

type I64Array []int64

func (self I64Array) Len() int { return len(self) }
func (self I64Array) Swap(i, j int) { self[i], self[j] = self[j], self[i] }
func (self I64Array) Less(i, j int) bool { return self[i] < self[j] }

func main() {

	rand.Seed(time.Now().UTC().UnixNano())

	dbPath := "./testdata/test_i64set.kv"
	pageSize := 4096


	for i:=0; i<128; i++ {
		TestSet(dbPath, pageSize)
	}


}

var testCounter = 0



func TestSet(dbPath string, pageSize int) {

	testCounter += 1

	var pid uint32
	metaOffset := 0

	//testData := make(map[int64]int64)
	var vals I64Array
	vals = testutils.RandI64Array(10000)

	for j:=0; j<8; j++ {

		testutils.OpenInternalPager(dbPath, pageSize, metaOffset, "w", func(pager gokvdb.IPager) {

			pid = pager.CreatePageId()
			set := gokvdb.NewLazyI64Set(pager, nil)

			for i, v := range vals {
				
				fmt.Printf("%04d i64Set i=%v add=%v\n", testCounter, i, v)
				set.Add(v)
			}

			meta := set.Save()
			pager.WritePayloadData(pid, meta)		

			fmt.Println("Save", set.ToString())
		})
	}

	sort.Sort(vals)

	testutils.OpenInternalPager(dbPath, pageSize, metaOffset, "r", func(pager gokvdb.IPager) {

		fmt.Println("Open I64Set", pid)

		meta, _ := pager.ReadPayloadData(pid)
		fmt.Println("meta", meta)

		set := gokvdb.NewLazyI64Set(pager, meta)

		fmt.Println("OpenSuccess", set.ToString())

		count := 0

		for v2 := range set.Values() {
			v := vals[count]
			isValid := v == v2
			fmt.Printf("%04d VALUES i=%07d v=%v v2=%v valid=%v\n", testCounter, count, v, v2, isValid)
			count += 1
			if !isValid {
				fmt.Println("VALID ERROR!")
				os.Exit(1)
			}
		}

		fmt.Println("VALID SUCCESS!", count)

	})
}




