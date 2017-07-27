gokvdb


內嵌 Key-Value 資料庫

Installation

	$ go get github.com/ahuilee/gokvdb


Example

	storage, err := gokvdb.OpenStorage(path)
	db, err := storage.DB("mydb")
	bt, err := db.OpenBTree(btName)

	bt.Set(123456, []byte("hello gokvdb!"))

	data, ok := bt.Get(123456)


