package gokvdb

import (
	"fmt"
	"bytes"
	"encoding/gob"
)

type SpaceList struct {
	pid uint32
	changed bool
	spaceByBranchId map[uint64]uint16
	//items []SpaceItem
}

type SpaceItem struct {
	branchId uint64
	spaceSize uint16
}


func _NewSpaceList(pid uint32) *SpaceList {

	sl := new(SpaceList)
	sl.pid = pid
	sl.spaceByBranchId = make(map[uint64]uint16)
	//sl.items = make([]SpaceItem, 0)

	return sl
}


func (sl *SpaceList) ToString() string {
	return fmt.Sprintf("<_NewSpaceList pid=%v>", sl.pid)
}

func (sl *SpaceList) Append(branchId uint64, valPage *ValPage) {
	spaceSize := PAGE_SIZE - valPage.CalcSize()
	if spaceSize >= 128 {		
		//fmt.Println("Append", branchId, "spaceSize", spaceSize)
		sl.spaceByBranchId[branchId] = uint16(spaceSize)
		//item := SpaceItem{branchId: branchId, spaceSize: uint16(spaceSize)}
		//sl.items = append(sl.items, item)
		//fmt.Println(sl.ToString(), "Append", item)
		sl.changed = true
	}
}

func (sl *SpaceList) Find(sizeRequired int) (uint64, bool) {
	_sizeRequired := uint16(sizeRequired)

	findLoop := 0

	for branchId, spaceSize := range sl.spaceByBranchId {
			//fmt.Println(sl.ToString(), "Find", i, item)
		findLoop += 1
		if _sizeRequired <= spaceSize {

			newSize := spaceSize - _sizeRequired
			
			if newSize < 128 {

				delete(sl.spaceByBranchId, branchId)

				//fmt.Println(sl.ToString(), "REMOVE", i, item, "newSize", newSize)
			} else {
				sl.spaceByBranchId[branchId] = newSize
			}

			sl.changed = true
			//fmt.Println("SpaceList Find loop", findLoop)	

			return branchId, true
		}

	}
	//fmt.Println("SpaceList Find loop", findLoop)	


	return 0, false
}


func _PackSpaceList(sl *SpaceList) []byte {
	metaData := _PackPgMeta(PgMeta{pid:sl.pid, pgType: PGTYPE_SPACELIST})

	w := NewDataStream()
	w.Write(metaData)

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(sl.spaceByBranchId)
	_CheckErr("_PackSpaceList", err)
	w.WriteChunk(buf.Bytes())

	/*w.WriteUInt32(uint32(len(sl.items)))
	for _, item := range sl.items {
		w.WriteUInt64(uint64(item.branchId))
		w.WriteUInt16(uint16(item.spaceSize))
	}*/

	return w.ToBytes()
}

func _UnpackSpaceList(pid uint32, data []byte) (*SpaceList, error) {
	sl := _NewSpaceList(pid)

	rd := NewDataStreamFromBuffer(data)
	chunk := rd.ReadChunk()
	var spaceByBranchId map[uint64]uint16
	buf := bytes.NewBuffer(chunk)
	dec := gob.NewDecoder(buf)
	err := dec.Decode(&spaceByBranchId)
	_CheckErr("_UnpackSpaceList", err)
	sl.spaceByBranchId = spaceByBranchId

	/*
	var branchId uint64
	var spaceSize uint16
	var items []SpaceItem
	var i uint32

	itemCount := rd.ReadUInt32()

	for i=0; i<itemCount; i++ {

		branchId = rd.ReadUInt64()
		spaceSize = rd.ReadUInt16()

		item := SpaceItem{branchId: branchId, spaceSize:spaceSize}
		items = append(items, item)
	}

	fmt.Println("LOAD SpaceItemS", len(items))
	sl.items = items*/

	return sl, nil
}

