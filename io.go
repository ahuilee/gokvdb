package gokvdb

import (
	//"os"
	"fmt"
	"bytes"
	//"encoding/binary"
	"encoding/gob"
)

type DBError struct {
	message string
}

func (e DBError) Error() string {
	return fmt.Sprintf("Error %s", e.message)
}
/*
func _Dumps(obj interface{}) ([]byte, error) {
	switch obj.(type) {
		case *KeyPage:
			return _PackKeyPage(obj.(*KeyPage)), nil
		case *ValPage:
			return _PackValPage(obj.(*ValPage)), nil
		case *HashNode:
			return _PackHashNode(obj.(*HashNode)), nil
		case *NonClusteredNode:
			return _PackNonClusteredNode(obj.(*NonClusteredNode)), nil
		case *SpaceList:
			return _PackSpaceList(obj.(*SpaceList)), nil
	}

	return nil, DBError{message: "no support"}
}*/
/*
func _SplitPageMeta(data []byte) (PgMeta, []byte) {

	buf := NewDataStreamFromBuffer(data)

	pid := buf.ReadUInt32()
	pgType := buf.ReadUInt8()

	return PgMeta{pid:pid, pgType:pgType}, data[5:]
}*/
/*
func _Loads(data []byte, pid uint32, pgType uint8) (interface{}, error) {

	pgMeta, dataBytes := _SplitPageMeta(data)	

	//fmt.Println("_Loads", "pgType", pgType, "pid", pid)

	err := pgMeta.Valid(pid, pgType)
	if err != nil {
		return nil, err
	}

	rd := NewDataStreamFromBuffer(dataBytes)


	switch pgType {
	case PGTYPE_KEY_PAGE:
		keyPage := _NewKeyPage()
		keyPage.pid = pid
		keyPage.infoByKey = make(map[string]KeyInfo)

		var rowCount uint32
		var rowKeyLen uint16
		var rowKey string
		var rowKeyId uint64
		var rowIsDeleted bool
		var rowBranchId uint64
		var i uint32

		rowCount = rd.ReadUInt32()
		//fmt.Println(">> LOAD KEYINFO", "rowCount", rowCount)

		for i=0; i<rowCount; i++ {
			rowKeyLen = rd.ReadUInt16()
			
			rowKeyBytes := rd.Read(int(rowKeyLen))
			rowKey = string(rowKeyBytes)

			rowKeyId = rd.ReadUInt64()
			rowIsDeleted = rd.ReadBool()
			rowBranchId = rd.ReadUInt64()
			
			info := KeyInfo{keyId: rowKeyId, isDeleted: rowIsDeleted, branchId: rowBranchId}
			keyPage.infoByKey[rowKey] = info
		}

		//fmt.Println(">>LOAD KEYPAGE SUCCESS!", pid)

		return keyPage, nil
	case PGTYPE_VAL_PAGE:
		valPage := _NewValPage()
		valPage.pid = pid
		valPage.valueByKeyId = make(map[uint64][]byte)
		rd := NewDataStreamFromBuffer(dataBytes)
		
		var i uint32
		var keyId uint64
		var rowBytes []byte

		rowCount := rd.ReadUInt24()

		//fmt.Println("UnpackValPage rowCount", rowCount)

		for i=0; i<rowCount; i++ {
			keyId = rd.ReadUInt64()
			rowBytes = rd.ReadChunk()
			//fmt.Println("ROW", keyId, len(rowBytes))
			valPage.valueByKeyId[keyId] = rowBytes
		}

		//fmt.Println("LOAD PGTYPE_VAL_PAGE", len(dataBytes))
		//valueByKeyIdChunk := rd.ReadChunk()

		//valueByKeyId := _UnpackBytes(valueByKeyIdChunk, valPage.valueByKeyId).(map[uint64][]byte)
		//fmt.Println("LOAD valueByRowId", valueByRowId)
		//valPage.valueByKeyId = valueByKeyId

		return valPage, nil
	case PGTYPE_HASH_NODE:
		node := new(HashNode)
		node.pid = pid

		//fmt.Println("PGTYPE_HASH_NODE >>", dataBytes)		
		node.mask = rd.ReadUInt32()
		contextBytes := rd.ReadChunk()		

		//fmt.Println("_LoadHashNode >> _LoadPayloadPage", len(dataBytes), "node.mask ", node.mask, "ReadChunk",  contextBytes)
		context := _UnpackBytes(contextBytes, node.context)
		node.context = context.(map[uint32]uint32)
		//fmt.Println(">> _LoadHashNode SUCCESS:")

		return node, nil
	case PGTYPE_NON_CLUSTERED_NODE:
		node := new(NonClusteredNode)
		node.pid = pid
		node.nodeType = rd.ReadUInt8()
		node.depth = rd.ReadUInt8()
		chunkBytes := rd.ReadChunk()
		//fmt.Println("_LoadNonClusteredNode >> _LoadPayloadPage", len(dataBytes))
		context := _UnpackBytes(chunkBytes, node.context)
		node.context = context.(NonClusteredContext)
		//fmt.Println(">> _LoadNonClusteredNode SUCCESS:")
		return node, nil

	case PGTYPE_SPACELIST:
		return _UnpackSpaceList(pid, dataBytes)

	}

	return nil, DBError{message: "no support"}
}*/


/*
func _PackValPage(p *ValPage) []byte {
	w := NewDataStream()
	w.Write(_PackPgMeta(PgMeta{pid:p.pid, pgType: PGTYPE_VAL_PAGE}))

	rowCount := uint32(len(p.valueByKeyId))
	//fmt.Println("_PackValPage rowCount", rowCount)
	w.WriteUInt24(rowCount)
	for k, v := range p.valueByKeyId {
		w.WriteUInt64(k)
		w.WriteChunk(v)
		//fmt.Println("ROW PACK", k, len(v))
	}
	//valueByKeyIdBytes := _Pack2Bytes(p.valueByKeyId)

	return w.ToBytes()
}

func _PackKeyPage(p *KeyPage) []byte {

	buf := NewDataStream()

	metaData := _PackPgMeta(PgMeta{pid:p.pid, pgType: PGTYPE_KEY_PAGE})

	buf.Write(metaData)	

	buf.WriteUInt32(uint32(len(p.infoByKey)))
	
	for k, info := range p.infoByKey {
		keyBytes := []byte(k)
		buf.WriteUInt16(uint16(len(keyBytes)))
		buf.Write(keyBytes)

		buf.WriteUInt64(info.keyId)
		buf.WriteBool(info.isDeleted)		
		buf.WriteUInt64(info.branchId)
	}	

	
	return buf.ToBytes()
}

func _PackHashNode(n *HashNode) []byte {
	metaData := _PackPgMeta(PgMeta{pid:n.pid, pgType: PGTYPE_HASH_NODE})

	buf := NewDataStream()
	buf.Write(metaData)
	buf.WriteUInt32(n.mask)

	contextData := _Pack2Bytes(n.context)
	//fmt.Println("_PackHashNode WriteChunk", contextData)
	buf.WriteChunk(contextData)

	output := buf.ToBytes()
	//fmt.Println("_PackHashNode output", output)
	
	return output
}

func _PackNonClusteredNode(n *NonClusteredNode) []byte {

	w := NewDataStream()
	metaData := _PackPgMeta(PgMeta{pid:n.pid, pgType: PGTYPE_NON_CLUSTERED_NODE})
	w.Write(metaData)
	w.WriteUInt8(n.nodeType)
	w.WriteUInt8(n.depth)

	contextData := _Pack2Bytes(n.context)
	w.WriteChunk(contextData)
	
	return w.ToBytes()
}

func _PackPgMeta(m PgMeta) []byte {

	buf := NewDataStreamFromBuffer(make([]byte, 5))

	buf.WriteUInt32(m.pid)
	buf.WriteUInt8(m.pgType)

	return buf.ToBytes()
}*/

func _Pack2Bytes(obj interface{}) []byte {

	var buf bytes.Buffer

	enc := gob.NewEncoder(&buf)
	err := enc.Encode(obj)
	_CheckErr("_Pack2Bytes", err)

	return buf.Bytes()
}
/*
func _UnpackBytes(data []byte, unpackType interface{}) interface{} {

	//fmt.Println("_UnpackBytes unpackType", unpackType)

	var err error

	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)	

	switch unpackType.(type) {
		case map[uint32]uint32:
			var dict map[uint32]uint32
			err = dec.Decode(&dict)
			_CheckErr("_UnpackBytes map[uint32]uint32", err)
			return dict
		case NonClusteredContext:
			//fmt.Println("_UnpackBytes NonClusteredContext")
			var dict NonClusteredContext
			err = dec.Decode(&dict)
			_CheckErr("_UnpackBytes map[uint64]uint64", err)
			return dict
			
		case map[uint64]uint64:
			fmt.Println("_UnpackBytes map[uint64]uint64")
			var dict map[uint64]uint64
			err = dec.Decode(&dict)
			_CheckErr("_UnpackBytes map[uint64]uint64", err)
			return dict
		case map[uint32]string:
			var dict map[uint32]string
			err = dec.Decode(&dict)
			_CheckErr("_UnpackBytes map[uint32]string", err)
			return dict
		case map[string][]byte:
			var obj map[string][]byte
			err = dec.Decode(&obj)
			_CheckErr("_UnpackBytes map[string][]byte", err)
			return obj

		case map[string]uint64:
			var obj map[string]uint64
			err = dec.Decode(&obj)
			_CheckErr("_UnpackBytes map[string]uint64", err)
			return obj
		case map[uint64][]uint8:
			var obj map[uint64][]uint8
			err = dec.Decode(&obj)
			_CheckErr("_UnpackBytes map[uint64][]uint8", err)
			return obj
	}

	return nil
}*/

type DataStream struct {
	buf []byte
	offset int
	isFixed bool
}


func NewDataStream() *DataStream {
	ds := new(DataStream)
	ds.buf = make([]byte, 1024)
	ds.offset = 0
	ds.isFixed = false

	return ds
}

func NewDataStreamFromBuffer(buffer []byte) *DataStream {
	ds := new(DataStream)
	ds.buf = buffer
	ds.offset = 0
	ds.isFixed = true
	return ds
}

func (ds *DataStream) ToBytes() []byte {

	size := ds.offset

	if ds.isFixed {
		size = len(ds.buf)
	}

	output := make([]byte, size)
	copy(output, ds.buf)

	return output
}

func (ds *DataStream) Seek(offset int) {
	ds.offset = offset
}

func (ds *DataStream) Read(count int) []byte {

	var value = make([]byte, count)
	copy(value, ds.buf[ds.offset:])

	ds.offset += count

	return value
}

func (ds *DataStream) ReadChunk()[]byte {
	var size = ds.ReadUInt24()
	output := ds.Read(int(size))
	return output
}

func (ds *DataStream) ReadBool() bool {
	value := ds.ReadUInt8()
	return value == 1
}

func (ds *DataStream) ReadUInt8() uint8 {
	var value byte
	value = ds.buf[ds.offset]
	ds.offset += 1

	return uint8(value)
}

func (ds *DataStream) ReadUInt16() uint16 {

	var value uint16
	data := ds.Read(2)
	value = uint16(data[0]) | uint16(data[1])<<8


	return value
}

func (ds *DataStream) ReadUInt24() uint32 {
	var value uint32
	data := ds.Read(3)
	value = uint32(data[0]) | uint32(data[1])<<8 | uint32(data[2])<<16
	return value
}

func (ds *DataStream) ReadUInt32() uint32 {
	var value uint32
	data := ds.Read(4)
	//fmt.Println("ReadUInt32", data)
	value = uint32(data[0]) | uint32(data[1]) << 8  | uint32(data[2]) << 16 | uint32(data[3]) << 24

	return value
}

func (ds *DataStream) ReadUInt64() uint64 {

	var value uint64
	data := ds.Read(8)
	value = uint64(data[0]) | uint64(data[1])<<8 | uint64(data[2])<<16 | uint64(data[3])<<24 | uint64(data[4])<<32 | uint64(data[5])<<40 | uint64(data[6])<<48 | uint64(data[7])<<56

	return value
}

func (ds *DataStream) _CheckSize(size int) {	

	if (ds.offset + size) > len(ds.buf) {
		if ds.isFixed {
			fmt.Println(fmt.Sprintf("DataStream is over fixed length %v", len(ds.buf)))
			return
		}
		appendSize := 4096
		if size > appendSize {
			appendSize = size
		}
		ds.buf = append(ds.buf, make([]byte, appendSize)...)
	}
}

func (ds *DataStream) Write(value []byte) {
	ds._CheckSize(len(value))
	/*
	for i:=0; i<len(value); i++ {
		ds.buf[ds.offset] = value[i]
		ds.offset += 1

	}*/
	copy(ds.buf[ds.offset:], value)
	ds.offset += len(value)
}

func (ds *DataStream) WriteChunk(value []byte) {
	ds.WriteUInt24(uint32(len(value)))
	ds.Write(value)
}

func (ds *DataStream) WriteBool(value bool) {

	if value {
		ds.WriteUInt8(1)	
	} else {
		ds.WriteUInt8(0)
	}
}

func (ds *DataStream) WriteUInt8(value uint8) {
	ds._CheckSize(1)
	ds.buf[ds.offset] = byte(value)
	ds.offset += 1
}

func (ds *DataStream) WriteUInt16(value uint16) {
	data := make([]byte, 2)
	data[0] = byte(value)
	data[1] = byte(value >> 8)
	ds.Write(data)
}

func (ds *DataStream) WriteUInt24(value uint32) {
	
	data := make([]byte, 3)
	data[0] = byte(value)
	data[1] = byte(value >> 8)
	data[2] = byte(value >> 16)	
	ds.Write(data)
}


func (ds *DataStream) WriteUInt32(value uint32) {
	data := make([]byte, 4)

	data[0] = byte(value)
	data[1] = byte(value >> 8)
	data[2] = byte(value >> 16)
	data[3] = byte(value >> 24)

	//fmt.Println("WriteUInt32", data)

	ds.Write(data)
}

func (ds *DataStream) WriteUInt64(value uint64) {

	data := make([]byte, 8)

	data[0] = byte(value)
	data[1] = byte(value >> 8)
	data[2] = byte(value >> 16)
	data[3] = byte(value >> 24)
	data[4] = byte(value >> 32)
	data[5] = byte(value >> 40)
	data[6] = byte(value >> 48)
	data[7] = byte(value >> 56)

	ds.Write(data)
}

func (ds *DataStream) WriteHStr(value string) {
	data := []byte(value)
	if len(data) > 65536 {
		data = data[:65536] 
	}

	dataLen := uint16(len(data))
	ds.WriteUInt16(dataLen)
	ds.Write(data)

	//fmt.Println("WriteHStr", dataLen, data, value)
}

func (ds *DataStream) ReadHStr() string {

	dataLen := ds.ReadUInt16()
	data := ds.Read(int(dataLen))

	//fmt.Println("ReadHStr", dataLen, data)

	return string(data)
}
