package main

import (
	"os"
	"fmt"
	"time"
	"math/rand"
	"strings"
	"github.com/satori/go.uuid"	
	"../../gokvdb"
)


func main() {

	dbpath := "./testdata/testdict"

	for i:=0; i<1; i++ {
		_TestUInt32StrDict(dbpath, "dict_uint32_str")
		//_TestStrUint32Dict(dbpath, "dict_str_uint32")
	}
	

}

func _TestUInt32StrDict(dbpath string, dictName string) {

	total := 0

	for i:=0; i<0; i++ {
		data := _RandDataUInt32Str(8192)

		_OpenUInt32Str(dbpath, dictName, func(d *gokvdb.LazyUInt32StrDict) {
			
			for k, v := range data {
				fmt.Println("SET", "_TestUInt32StrDict", total, "SET", k, v)
				d.Set(k, v)
				total += 1
			}
		})

		_TestValidUInt32Str(dbpath, dictName, data)
	}

	for i:=0; i<64; i++ {
		data := _RandDataUInt32Str(8192)

		t1 := time.Now()
		_OpenUInt32Str(dbpath, dictName, func(d *gokvdb.LazyUInt32StrDict) {
			fmt.Println("UPDATE", len(data))
			d.Update(data)
		})
		dt := time.Since(t1)
		fmt.Println("UPDATE", len(data), "dt", dt)
		_TestValidUInt32Str(dbpath, dictName, data)
	}

	fmt.Println(strings.Repeat("-", 50))
	_OpenUInt32Str(dbpath, dictName, func(d *gokvdb.LazyUInt32StrDict) {
		count := 0
		for item := range d.Items() {
			count += 1
			fmt.Println("Items", fmt.Sprintf("%07d", count), item.Key(), item.Value())
		}
	})
}

func _RandDataUInt32Str(testCount int) map[uint32]string {
	var data = make(map[uint32]string)

	for i:=0; i<testCount; i++ {
		key := uint32(rand.Int31())
		val := fmt.Sprintf("key-%v", uuid.NewV4())
		data[key] = val
	}

	return data
}

func _OpenStorage(dbpath string, callback func(*gokvdb.LazyStorage)) {
	s, err := gokvdb.OpenLazyStorage(dbpath)
	if err == nil {

		callback(s)
		s.Close()
	}
}

func _OpenUInt32Str(dbpath string, dictName string, callback func(*gokvdb.LazyUInt32StrDict)) {

	_OpenStorage(dbpath, func(s *gokvdb.LazyStorage) {
		dict := s.NewUInt32StrDict(dictName)

		callback(dict)

	})

}

func _OpenStrUInt32(dbpath string, dictName string, callback func(*gokvdb.LazyStrUInt32Dict)) {

	_OpenStorage(dbpath, func(s *gokvdb.LazyStorage) {
		dict := s.NewStrUInt32Dict(dictName)

		callback(dict)

	})

}


func _TestValidUInt32Str(dbpath string, name string, data map[uint32]string) {

	_OpenUInt32Str(dbpath, name, func(dict *gokvdb.LazyUInt32StrDict){

		
	for k, v := range data {
		
		val, ok := dict.Get(k)
		fmt.Println("GET VALID", "key=", k, "val=", val, ok, v == val)
		if !ok {
			fmt.Println("!!!TEST err")
			os.Exit(1)
		}
	}

	})


}




func _TestStrUint32Dict(dbpath string, dictName string) {

	total := 0

	for i:=0; i<1 ; i++ {
		data := _RandDataStrUInt32(8192)
		_OpenStrUInt32(dbpath, dictName, func(d*gokvdb.LazyStrUInt32Dict) {

			for k, v := range data {
				total += 1
				fmt.Println("_TestStrUint32Dict SET", total, k, v)
				d.Set(k, v)
			}
		})

		_TestValid(dbpath, dictName, data)
	}

	
	for i:=0; i<8; i++ {
		data := _RandDataStrUInt32(8192)
		_OpenStrUInt32(dbpath, dictName, func(d*gokvdb.LazyStrUInt32Dict) {
			fmt.Println("TEST UPDATE", len(data))
			t1 := time.Now()
			d.Update(data)
			dt := time.Since(t1)
			fmt.Println("dt", dt)
		})

		_TestValid(dbpath, dictName, data)
	}

	fmt.Println(strings.Repeat("-", 50))
	_OpenStrUInt32(dbpath, dictName, func(d *gokvdb.LazyStrUInt32Dict) {
		count := 0
		t1 := time.Now()
		for item := range d.Items() {
			count += 1
			fmt.Println("Items", fmt.Sprintf("%07d", count), item.Key(), item.Value())
		}

		dt := time.Since(t1)
		fmt.Println("dt", dt)
	})

}

func _TestValid(dbpath string, name string, data map[string]uint32) {

	_OpenStorage(dbpath, func(s *gokvdb.LazyStorage) {
		dict := s.NewStrUInt32Dict(name)
		for k, v := range data {
			fmt.Println("GET", k)
			val, ok := dict.Get(k)
			fmt.Println("GET VALID", k, val, ok, v == val)
			if !ok {
				fmt.Println("TEST err")
				os.Exit(1)
			}
		}
	})
	

}

func _RandDataStrUInt32(testCount int) map[string]uint32 {
	var data = make(map[string]uint32)
	for i:=0; i<testCount; i++ {
		key := fmt.Sprintf("key-%v", uuid.NewV4())
		val := uint32(rand.Int31())

		data[key] = val
	}
	return data
}
