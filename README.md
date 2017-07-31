gokvdb

練習用 golang 開發 嵌入式 Key-Value 資料庫

Installation

	$ go get github.com/ahuilee/gokvdb


Example
	

	storage, _ := gokvdb.OpenStorage("path")

String by int64 

	nameByIdDict := gokvdb.NewI64StrDict(storage, "mydb", "nameByIdDict")

	nameByIdDict.Set(1, "name1")
	nameByIdDict.Set(2, "name2")

	nameByIdDict.Save(true)

	txt1, _ := nameByIdDict.Get(1)
	txt2, _ := nameByIdDict.Get(2)
	// txt1 == "name1"
	// txt2 == "name2"

	for item := range nameByIdDict.Items() {
		fmt.Printf("key=%v value=%v", item.Key(), item.Value())
	}
	// key=1 value=name1
	// key=2 value=name2

Int64 by string

	idByNameDict := gokvdb.NewStrI64Dict(storage, "mydb", "idByNameDict")

	idByNameDict.Set("name1", 123456)
	idByNameDict.Set("name2", 654321)

	idByNameDict.Save(true)

	for item := range nameByIdDict.Items() {
		fmt.Printf("key=%v value=%v", item.Key(), item.Value())
	}

	// key=name1 value=123456
	// key=name2 value=654321

	


