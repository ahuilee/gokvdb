package gokvdb

import (
	"os"
	"fmt"
	//"sync"
	"path/filepath"
)


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
