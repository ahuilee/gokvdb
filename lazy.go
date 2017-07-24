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

type LazyUInt32StrDict struct {
	name string
	storage *LazyStorage
	meta *LazyUInt32StrDictMeta
	metaChanged bool
	pages map[string]*LazyUInt32StrDictPage
}


type LazyUInt32StrDictPage struct {
	context map[uint32]string
	changed bool
}

type LazyUInt32StrDictMeta struct {
	lastBranchPageId uint32
	pageKeyByBranchId map[uint32]string
}



func OpenLazyStorage(dbpath string) (*LazyStorage, error) {
	s := new(LazyStorage)
	db, err := OpenHash(dbpath)
	if err != nil {
		return nil, err
	}
	s.db = db

	return s, nil
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
	dict.metaChanged = false

	return dict
}

/* LazyStrUInt32Dict */


type LazyStrUInt32Dict struct {
	name string
	storage *LazyStorage
	meta *LazyStrUInt32DictMeta
	metaChanged bool
	pages map[string]*LazyStrUInt32DictPage
}

type LazyStrUInt32DictPage struct {
	context map[string]uint32
	changed bool
}

type LazyStrUInt32DictMeta struct {
	lastBranchPageId uint32
	pageKeyByHashKey map[uint32]string
	changed bool
}


type StrUInt32Item struct {
	key string
	value uint32
}

func (i *StrUInt32Item) Key() string {
	return i.key
}

func (i *StrUInt32Item) Value() uint32 {
	return i.value
}

func (d *LazyStrUInt32Dict) Items() chan StrUInt32Item {
	q := make(chan StrUInt32Item)

	go func(ch chan StrUInt32Item) {

		meta := d._GetMeta()
		for _, pgKey := range meta.pageKeyByHashKey {
			page := d._GetPageByPageKey(pgKey)
			for k, v := range page.context {
				ch <- StrUInt32Item{key:k, value:v}
			}
		}

		close(ch)

	} (q)

	return q
}


func (d *LazyStrUInt32Dict) Get(key string) (uint32, bool) {
	page := d._GetPage(key)

	val, ok := page.context[key]

	return val, ok
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

func (d *LazyStrUInt32Dict) _HashKey(key string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32() & 0xff
}

func (d *LazyStrUInt32Dict) _GetPageKey(key string) string {
	hashKey := d._HashKey(key)

	meta := d._GetMeta()
	pageKey, ok := meta.pageKeyByHashKey[hashKey]
	if !ok {
		branchPageId := meta.lastBranchPageId + 1
		meta.lastBranchPageId = branchPageId
		pageKey = fmt.Sprintf("%s_b%06d", d.name, branchPageId)
		meta.pageKeyByHashKey[hashKey] = pageKey
		d.metaChanged = true
	}

	return pageKey
}

func (d *LazyStrUInt32Dict) _GetPage(key string) *LazyStrUInt32DictPage {
	pageKey := d._GetPageKey(key)

	return d._GetPageByPageKey(pageKey)
}

func (d *LazyStrUInt32Dict) _GetPageByPageKey(pageKey string) *LazyStrUInt32DictPage {

	page, ok := d.pages[pageKey]
	if !ok {
		page = new(LazyStrUInt32DictPage)

		var context map[string]uint32

		pageData, ok := d.storage.db.Get(pageKey)
		if ok {
			ds := NewDataStreamFromBuffer(pageData)
			chunk := ds.ReadChunk()
			buf := bytes.NewBuffer(chunk)
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


func (d *LazyStrUInt32Dict) Save() {

	for pgKey, pg := range d.pages {
		if pg.changed {
			//fmt.Println("LazyStrUInt32Dict Save", pg)
			pg.changed = false

			var buf bytes.Buffer
			enc := gob.NewEncoder(&buf)
			err := enc.Encode(pg.context)
			_CheckErr("LazyStrUInt32Dict Save", err)
			w := NewDataStream()
			w.WriteChunk(buf.Bytes())
			d.storage.db.Set(pgKey, w.ToBytes())
		}
	}

	if d.metaChanged {
		d.metaChanged = false
		fmt.Println("LazyStrUInt32Dict SAVE META", len(d.meta.pageKeyByHashKey))

		w := NewDataStream()
		w.WriteUInt32(d.meta.lastBranchPageId)

		var buf bytes.Buffer

		enc := gob.NewEncoder(&buf)
		err := enc.Encode(d.meta.pageKeyByHashKey)
		_CheckErr("LazyStrUInt32Dict Save META", err)
	
		w.WriteChunk(buf.Bytes())
		d.storage.db.Set(d._GetMetaPageKey(), w.ToBytes())
	}
}

func (d *LazyStrUInt32Dict) _GetMetaPageKey() string {
	return fmt.Sprintf("%s_META", d.name)
}

func (d *LazyStrUInt32Dict) _GetMeta() *LazyStrUInt32DictMeta {
	if d.meta == nil {
		meta := new(LazyStrUInt32DictMeta)

		pageData, ok := d.storage.db.Get(d._GetMetaPageKey())

		//fmt.Println("_GetMeta", ok, pageData)
		if ok {

			var lastBranchPageId uint32			
			var pageKeyByHashKey map[uint32]string

			rd := NewDataStreamFromBuffer(pageData)

			lastBranchPageId = rd.ReadUInt32()
			chunk := rd.ReadChunk()
		
			buf := bytes.NewBuffer(chunk)
			dec := gob.NewDecoder(buf)
			err := dec.Decode(&pageKeyByHashKey)
			_CheckErr("LazyStrUInt32Dict GET SCHEMA", err)	
			
			meta.pageKeyByHashKey = pageKeyByHashKey
			meta.lastBranchPageId = lastBranchPageId
			//fmt.Println("GetSchema SUCCESS", meta)
		} else {
			
			meta.pageKeyByHashKey = make(map[uint32]string)
			meta.lastBranchPageId = 0
			d.metaChanged = true
		}

		d.meta = meta
	}

	return d.meta
}


/* LazyUInt32StrDict */

func (d *LazyUInt32StrDict) Set(key uint32, value string) {
	d._Set(key, value)
	d.Save()
}


func (d *LazyUInt32StrDict) Get(key uint32) (string, bool) {
	page := d._GetPage(key)
	val, ok := page.context[key]
	return val, ok
}

func (d *LazyUInt32StrDict) _GetPageKey(key uint32) string {
	branchId := key / 256
	meta := d._GetMeta()
	//fmt.Println("_GetPageKey", meta)
	pageKey, ok := meta.pageKeyByBranchId[branchId]
	if !ok {
		branchPageId := meta.lastBranchPageId + 1
		meta.lastBranchPageId = branchPageId
		pageKey = fmt.Sprintf("%s_b%06d", d.name, branchPageId)
		meta.pageKeyByBranchId[branchId] = pageKey
		d.metaChanged = true
	}

	return pageKey
}

func (d *LazyUInt32StrDict) _GetPage(key uint32) *LazyUInt32StrDictPage {

	pageKey := d._GetPageKey(key)

	return d._GetPageByPageKey(pageKey)
}

func (d *LazyUInt32StrDict) _GetPageByPageKey(pageKey string) *LazyUInt32StrDictPage {
	page, ok := d.pages[pageKey]
	if !ok {
		page = new(LazyUInt32StrDictPage)
		var context map[uint32]string

		pageData, ok := d.storage.db.Get(pageKey)
		if ok {
			rd := NewDataStreamFromBuffer(pageData)
			chunk := rd.ReadChunk()
			buf := bytes.NewBuffer(chunk)
			dec := gob.NewDecoder(buf)
			err := dec.Decode(&context)
			_CheckErr("LazyUInt32StrDict _GetPage", err)

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

func (d *LazyUInt32StrDict) Update(data map[uint32]string) {
	for k, v := range data {
		d._Set(k, v)
	}
	d.Save()
}

func (d *LazyUInt32StrDict) _GetMetaPageKey() string {
	return fmt.Sprintf("%s_META", d.name)
}

func (d *LazyUInt32StrDict) _GetMeta() *LazyUInt32StrDictMeta {
	if d.meta == nil {
		meta := new(LazyUInt32StrDictMeta)

		pageData, ok := d.storage.db.Get(d._GetMetaPageKey())

		//fmt.Println("_GetMeta", ok, pageData)
		if ok {

			rd := NewDataStreamFromBuffer(pageData)
			lastBranchPageId := rd.ReadUInt32()
			chunk := rd.ReadChunk()
			
			var pageKeyByBranchId map[uint32]string
			buf := bytes.NewBuffer(chunk)
			dec := gob.NewDecoder(buf)
			err := dec.Decode(&pageKeyByBranchId)
			_CheckErr("LazyUInt32StrDict GET SCHEMA", err)	
			meta.lastBranchPageId = lastBranchPageId
			meta.pageKeyByBranchId = pageKeyByBranchId
			//fmt.Println("GetSchema SUCCESS", meta)
		} else {
			
			meta.lastBranchPageId = 0
			meta.pageKeyByBranchId = make(map[uint32]string)
			d.metaChanged = true
		}

		d.meta = meta
	}

	return d.meta
}

type ItemUInt32Str struct {
	key uint32
	value string
}

func (i *ItemUInt32Str) Key() uint32 {
	return i.key
}

func (i *ItemUInt32Str) Value() string {
	return i.value
}

func (d *LazyUInt32StrDict) Items() chan ItemUInt32Str {

	q := make(chan ItemUInt32Str)
	go func(ch chan ItemUInt32Str) {
		
		meta := d._GetMeta()

		fmt.Println("Items meta.pageKeyByBranchId", len(meta.pageKeyByBranchId))

		for _, pgKey := range meta.pageKeyByBranchId {

			//fmt.Println("Items", pgKey)

			page := d._GetPageByPageKey(pgKey)

			for k, v := range page.context {
				ch <- ItemUInt32Str{key: k, value: v}
			}			
		}

		close(ch)

	}(q)

	return q
}

func (d *LazyUInt32StrDict) Save() {

	changedPgCount := 0

	for pgKey, pg := range d.pages {
		if pg.changed {
			//fmt.Println("LazyUInt32StrDict Save", pgKey, len(pg.context))
			changedPgCount += 1
			pg.changed = false
			var buf bytes.Buffer
			enc := gob.NewEncoder(&buf)
			err := enc.Encode(pg.context)
			_CheckErr("LazyUInt32StrDict Save", err)
			w := NewDataStream()
			w.WriteChunk(buf.Bytes())
			d.storage.db.Set(pgKey, w.ToBytes())
		}
	}

	fmt.Println("changedPgCount", changedPgCount)

	if d.metaChanged {
		d.metaChanged = false
		fmt.Println("LazyUInt32StrDict SAVE META", len(d.meta.pageKeyByBranchId))

		w := NewDataStream()
		w.WriteUInt32(d.meta.lastBranchPageId)

		var buf bytes.Buffer

		enc := gob.NewEncoder(&buf)
		err := enc.Encode(d.meta.pageKeyByBranchId)
		_CheckErr("LazyUInt32StrDict Save META", err)

		w.WriteChunk(buf.Bytes())
		
		d.storage.db.Set(d._GetMetaPageKey(), w.ToBytes())

	}
}

