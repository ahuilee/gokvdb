package main

import (
	"os"
	"fmt"
	"math/rand"
	"github.com/satori/go.uuid"	
	"../../gokvdb"
)


func main() {

	dbpath := "./testdata/testdict"
	//var s *gokvdb.LazyStorage

	//s := gokvdb.OpenLazyStorage(dbpath)

	//_TestUInt32StrDict(dbpath, "dict_uint32_str")
	_TestStrUint32Dict(dbpath, "dict_str_uint32")
	

}

func _TestUInt32StrDict(dbpath string, dictName string) {

	for i:=0; i<1; i++ {
		data := _RandDataUInt32Str(8192)

		_OpenUInt32Str(dbpath, dictName, func(d *gokvdb.LazyUInt32StrDict) {
			j := 0
			for k, v := range data {
				fmt.Println(j, "SET", k, v)
				d.Set(k, v)
				j += 1
			}
		})

		_TestValidUInt32Str(dbpath, dictName, data)
	}

	for i:=0; i<1; i++ {
		data := _RandDataUInt32Str(8192)
		_OpenUInt32Str(dbpath, dictName, func(d *gokvdb.LazyUInt32StrDict) {
			fmt.Println("UPDATE", data)
			d.Update(data)
		})
		_TestValidUInt32Str(dbpath, dictName, data)
	}
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

func _OpenUInt32Str(dbpath string, dictName string, callback func(*gokvdb.LazyUInt32StrDict)) {

	s := gokvdb.OpenLazyStorage(dbpath)
	dict := s.NewUInt32StrDict(dictName)

	callback(dict)
	s.Close()
}


func _TestValidUInt32Str(dbpath string, name string, data map[uint32]string) {
	s := gokvdb.OpenLazyStorage(dbpath)
	dict := s.NewUInt32StrDict(name)

	for k, v := range data {
		fmt.Println("GET", k)
		val, ok := dict.Get(k)
		fmt.Println("GET VALID", k, val, ok, v == val)
		if !ok {
			fmt.Println("TEST err")
			os.Exit(1)
		}
	}
}


func _OpenStrUInt32(dbpath string, dictName string, callback func(*gokvdb.LazyStrUInt32Dict)) {

	s := gokvdb.OpenLazyStorage(dbpath)
	dict := s.NewStrUInt32Dict(dictName)

	callback(dict)
	s.Close()
}

func _TestStrUint32Dict(dbpath string, dictName string) {

	for i:=0; i<1 ; i++ {
		data := _RandDataStrUInt32(8192)
		_OpenStrUInt32(dbpath, dictName, func(d*gokvdb.LazyStrUInt32Dict) {

			for k, v := range data {
				fmt.Println("_TestStrUint32Dict SET", k, v)
				d.Set(k, v)
			}
		})

		_TestValid(dbpath, dictName, data)
	}

	
	for i:=0; i<1; i++ {
		data := _RandDataStrUInt32(8192)
		_OpenStrUInt32(dbpath, dictName, func(d*gokvdb.LazyStrUInt32Dict) {
			fmt.Println("TEST UPDATE", data)
			d.Update(data)
		})

		_TestValid(dbpath, dictName, data)
	}

}

func _TestValid(dbpath string, name string, data map[string]uint32) {
	s := gokvdb.OpenLazyStorage(dbpath)
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
