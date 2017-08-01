package gokvdb

import (
	"os"
	"fmt"
	//"sync"
	"path/filepath"
)


type LazyList struct {
	arr []interface{}
	offset int
}

func NewLazyList() *LazyList {
	self := new(LazyList)
	self.arr = make([]interface{}, 32)
	self.offset = 0
	return self
}

func (self *LazyList) Append(item interface{}) {
	if self.offset >= len(self.arr) {
		self.arr = append(self.arr, make([]interface{}, 32)...)
	}
	self.arr[self.offset] = item
	self.offset += 1
}

func (self *LazyList) Pop() interface{} {
	if self.offset > 0 {
		self.offset -= 1
		item := self.arr[self.offset]
		return item
	}
	return nil
}

func (self *LazyList) Len() int {
	return self.offset
}

func _CheckErr(message string, err error) {
	if err != nil {
		fmt.Println(message, "ERROR", err)
		os.Exit(1)
	}
}

func _CheckCreateDirpath(fullpath string) {

	dirpath := filepath.Dir(fullpath)
	_, err := os.Stat(dirpath)
	if err != nil {
		if os.IsNotExist(err) {
			os.MkdirAll(dirpath, os.ModePerm)
		}
	}
}
