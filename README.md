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

	
Int64Set by int64

	friendSetByActId := gokvdb.NewLazyI64I64SetDict(storage, "mydb", "friendSetByActId")

	friendSetByActId.Add(1, 2)
	friendSetByActId.Add(1, 2)
	friendSetByActId.Add(1, 3)
	friendSetByActId.Add(1, 3)
	friendSetByActId.Add(1, 4)
	friendSetByActId.Add(1, 5)

	friendSetByActId.Save(true)

	result, _ := friendSetByActId.Get(1)

	fmt.Println(result.Values())
	// output: 2, 3, 4, 5


Int64Set by string

	intSetByName := gokvdb.NewLazyI64I64SetDict(storage, "mydb", "intSetByName")

	intSetByName.Add("taiwan", 1)
	intSetByName.Add("taiwan", 1)
	intSetByName.Add("taiwan", 2)
	intSetByName.Add("taiwan", 2)
	intSetByName.Add("taiwan", 3)
	intSetByName.Add("taiwan", 3)
	intSetByName.Add("taipei", 5)
	intSetByName.Add("taipei", 6)
	intSetByName.Add("taipei", 7)
	intSetByName.Add("taipei", 7)

	intSetByName.Save(true)

	for item := range intSetByName.Items() {
		fmt.Println(item.Key(), item.Values())
	}

	// output:
	// taiwan 1
	// taiwan 2
	// taiwan 3
	// taipei 5
	// taipei 6
	// taipei 7

	

