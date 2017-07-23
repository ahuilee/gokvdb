package gokvdb

import (
	"fmt"
	"bytes"
	"encoding/gob"
	"hash/fnv"
)


type LazyStorage struct {
	db *DB
}

type LazyStrUInt32DictPage struct {
	context map[string]uint32
	changed bool
}

type LazyStrUInt32Dict struct {
	name string
	storage *LazyStorage
	pages map[string]*LazyStrUInt32DictPage
}

type LazyUInt32StrDictPage struct {
	context map[uint32]string
	changed bool
}

type LazyUInt32StrDict struct {
	name string
	storage *LazyStorage
	pages map[string]*LazyUInt32StrDictPage
}



func OpenLazyStorage(dbpath string) *LazyStorage {
	s := new(LazyStorage)
	db := OpenHash(dbpath)
	s.db = db

	return s
}

func (s *LazyStorage) Close() {
	if s.db != nil {
		s.db.Close()
		s.db = nil
	}
}

func (s *LazyStorage) NewStrUInt32Dict(name string) *LazyStrUInt32Dict {

	dict := new(LazyStrUInt32Dict)
	dict.name = name
	dict.storage = s
	dict.pages = make(map[string]*LazyStrUInt32DictPage)

	return dict
}

func (s *LazyStorage) NewUInt32StrDict(name string) *LazyUInt32StrDict {

	dict := new(LazyUInt32StrDict)
	dict.name = name
	dict.storage = s
	dict.pages = make(map[string]*LazyUInt32StrDictPage)

	return dict
}




func (d *LazyStrUInt32Dict) Get(key string) (uint32, bool) {
	page := d._GetPage(key)

	val, ok := page.context[key]

	return val, ok
}

func (d *LazyStrUInt32Dict) _HashKey(key string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32() & 0xff
}

func (d *LazyStrUInt32Dict) _GetPageKey(key string) string {
	hashKey := d._HashKey(key)

	return fmt.Sprintf("%s_p%06d", d.name, hashKey)
}

func (d *LazyStrUInt32Dict) _GetPage(key string) *LazyStrUInt32DictPage {

	pageKey := d._GetPageKey(key)

	page, ok := d.pages[pageKey]
	if !ok {
		page = new(LazyStrUInt32DictPage)

		var context map[string]uint32

		pageData, ok := d.storage.db.Get(pageKey)
		if ok {
			buf := bytes.NewBuffer(pageData)
			dec := gob.NewDecoder(buf)
			err := dec.Decode(&context)
			_CheckErr("LazyStrUInt32Dict _GetPage", err)

		} else {
			context = make(map[string]uint32)
		}
		
		page.context = context
		d.pages[pageKey] = page
	}

	return page
}

func (d *LazyStrUInt32Dict) _Set(key string, value uint32) {
	page := d._GetPage(key)
	page.context[key] = value
	page.changed = true
	
}

func (d *LazyStrUInt32Dict) Set(key string, value uint32) {
	d._Set(key, value)
	d.Save()
}

func (d *LazyStrUInt32Dict) Update(data map[string]uint32) {
	for k, v := range data {
		d._Set(k, v)
	}
	d.Save()
}

func (d *LazyStrUInt32Dict) Save() {

	for pgKey, pg := range d.pages {
		if pg.changed {
			//fmt.Println("LazyStrUInt32Dict Save", pg)
			pg.changed = false
			var buf bytes.Buffer
			enc := gob.NewEncoder(&buf)
			err := enc.Encode(pg.context)
			_CheckErr("LazyStrUInt32Dict Save", err)
			d.storage.db.Set(pgKey, buf.Bytes())
		}
	}
}


/* LazyUInt32StrDict */


func (d *LazyUInt32StrDict) Get(key uint32) (string, bool) {
	page := d._GetPage(key)
	val, ok := page.context[key]
	return val, ok
}

func (d *LazyUInt32StrDict) _GetPageKey(key uint32) string {
	branchKey := key / 256
	return fmt.Sprintf("%s_p%06d", d.name, branchKey)
}

func (d *LazyUInt32StrDict) _GetPage(key uint32) *LazyUInt32StrDictPage {

	pageKey := d._GetPageKey(key)
	page, ok := d.pages[pageKey]
	if !ok {
		page = new(LazyUInt32StrDictPage)
		var context map[uint32]string

		pageData, ok := d.storage.db.Get(pageKey)
		if ok {
			buf := bytes.NewBuffer(pageData)
			dec := gob.NewDecoder(buf)
			err := dec.Decode(&context)
			_CheckErr("LazyStrUInt32Dict _GetPage", err)

		} else {
			context = make(map[uint32]string)
		}
		
		page.context = context
		d.pages[pageKey] = page
	}

	return page
}

func (d *LazyUInt32StrDict) _Set(key uint32, value string) {
	page := d._GetPage(key)
	page.context[key] = value
	page.changed = true
	
}

func (d *LazyUInt32StrDict) Set(key uint32, value string) {
	d._Set(key, value)
	d.Save()
}

func (d *LazyUInt32StrDict) Update(data map[uint32]string) {
	for k, v := range data {
		d._Set(k, v)
	}
	d.Save()
}

func (d *LazyUInt32StrDict) Save() {

	for pgKey, pg := range d.pages {
		if pg.changed {
			//fmt.Println("LazyStrUInt32Dict Save", pg)
			pg.changed = false
			var buf bytes.Buffer
			enc := gob.NewEncoder(&buf)
			err := enc.Encode(pg.context)
			_CheckErr("LazyStrUInt32Dict Save", err)
			d.storage.db.Set(pgKey, buf.Bytes())
		}
	}
}
