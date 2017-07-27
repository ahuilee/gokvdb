package gokvdb

import (
	"os"
	"fmt"
	//"sync"
	//"path/filepath"
)


func _CheckErr(message string, err error) {
	if err != nil {
		fmt.Println(message, "ERROR", err)
		os.Exit(1)
	}
}

