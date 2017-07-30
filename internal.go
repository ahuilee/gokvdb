package gokvdb

import (
	"os"
	"fmt"
	"sync"
)

const (
	INTERNAL_PAGE_HEADER_SIZE int = 16
	INTERNAL_PAGER_META_SIZE = 24

)

type InternalPager struct {

	pageSize uint16
	lastPageId uint32
	rootPageId uint32
	freelistPageId uint32
	pager IPager	
	freelist *FreePageList
	root map[uint32]uint32
	branchPages map[uint32]*InternalBranchPage
	contextByPageId map[uint32]*InternalDataContext
	payloadFactory *PayloadPageFactory
	isChanged bool
	rwlock sync.Mutex
}

type InternalBranchPage struct {
	pid uint32
	contextPageIdByBranchKey map[uint32]uint32	
	isChanged bool
}

type InternalDataContext struct {
	pid uint32
	dataByPageId map[uint32][]byte
	isChanged bool
}

func NewInternalPager(pager IPager, pageSize uint16, meta []byte) IPager {

	ip := new(InternalPager)
	ip.pager = pager
	ip.root = make(map[uint32]uint32)
	ip.pageSize = pageSize
	ip.lastPageId = 0
	ip.rootPageId = 0
	ip.freelistPageId = 0
	ip.branchPages = make(map[uint32]*InternalBranchPage)
	ip.contextByPageId = make(map[uint32]*InternalDataContext)
	ip.isChanged = false

	

	if meta != nil && len(meta) >= INTERNAL_PAGER_META_SIZE {

		metaR := NewDataStreamFromBuffer(meta)
		ip.pageSize = metaR.ReadUInt16()
		ip.lastPageId = metaR.ReadUInt32()
		ip.rootPageId = metaR.ReadUInt32()
		ip.freelistPageId = metaR.ReadUInt32()

		//fmt.Println("NewInternalPager.freelistPageId=", ip.freelistPageId)

	}

	if ip.rootPageId == 0 {
		rootPageId := pager.CreatePageId()
		ip.rootPageId = rootPageId
		ip.isChanged = true
	} else {

		rootData, err := pager.ReadPayloadData(ip.rootPageId)
		if err == nil {
			rootRd := NewDataStreamFromBuffer(rootData)			
			var branchRootKey uint32
			var branchPageId uint32

			rowsCount := int(rootRd.ReadUInt32())

			for i:=0; i<rowsCount; i++ {
				branchRootKey = rootRd.ReadUInt32()
				branchPageId = rootRd.ReadUInt32()

				ip.root[branchRootKey] = branchPageId

				//fmt.Println("ROOT ITEM", "branchRootKey=", branchRootKey, "branchPageId=", branchPageId)			
			}
			//fmt.Println("INTERNAL ROOT", len(ip.root))
		}
	}

	

	freelist := _NewFreePageList(pager, ip.freelistPageId, false)
	ip.freelist = freelist
	ip.freelistPageId = freelist.rootPageId
	ip.payloadFactory = NewPayloadPageFactory(ip)
	ip.payloadFactory.isDebug = false


	return ip
}

func (p *InternalPager) ToString() string {
	return fmt.Sprintf("<InternalPager pageSize=%v lastPageId=%v rootPageId=%v>", p.pageSize, p.lastPageId, p.rootPageId)
}

func (p *InternalPager) CreatePageId() uint32 {
	
	freeId, ok := p.freelist.Pop()
	if ok {
		//fmt.Println(">> InternalPager CreatePageId Free", freeId)
		return freeId
	}

	//fmt.Println("InternalPager CreatePageId New")
	pid := p.lastPageId + 1
	p.lastPageId = pid

	return pid
}

func (p *InternalPager) FreePageId(pid uint32) {

	//fmt.Println(">>>>>>> InternalPager FreePageId", pid)

	empty := make([]byte, INTERNAL_PAGE_HEADER_SIZE)

	p.WritePage(pid, empty)
	p.freelist.Put(pid)
}

func (p *InternalPager) GetPageSize() int {
	return int(p.pageSize)
}

func (p *InternalPager) _GetBranchKeys(pid uint32) (uint32, uint32) {
	branchKey := pid / 24
	branchRootKey := branchKey / 1024

	return branchRootKey, branchKey
}

func (p *InternalPager) WritePayloadData(pid uint32, data []byte) {
	//fmt.Println("InternalPager WritePayloadData", pid)
	p.payloadFactory.WritePayloadData(pid, data)
}

func (p *InternalPager) ReadPayloadData(pid uint32) ([]byte, error) {
	return p.payloadFactory.ReadPayloadData(pid)
}


func (p *InternalPager) ReadPage(pid uint32, count int) ([]byte, error) {

	p.rwlock.Lock()
	defer p.rwlock.Unlock()

	branchRootKey, branchKey := p._GetBranchKeys(pid)
	
	branchPageId, ok := p.root[branchRootKey]

	//fmt.Printf("ReadPage pid=%v branchPageId=%v ok=%v\n", pid, branchPageId, ok)
	if ok {

		branchPage := p._GetBranchPageByPageId(branchPageId)

		contextPageId, ok := branchPage.contextPageIdByBranchKey[branchKey]
		//fmt.Printf("ReadPage pid=%v branchPageId=%v ok=%v contextPageId=%v\n", pid, branchPageId, ok, contextPageId)
		if ok {
			context, ok := p.contextByPageId[contextPageId]
			if !ok {
				context = p._ReadDataContext(contextPageId)
				p.contextByPageId[context.pid] = context			
			}

			data, ok := context.dataByPageId[pid]
			if ok {
				_count := count
				if _count < 1 {
					_count = int(p.pageSize)
				}
				output := make([]byte, _count)
				copy(output, data)
				return output, nil
			}
		}
		
	}

	return nil, DBError{message: "no data"}
}

func (p *InternalPager) WritePage(pid uint32, data []byte) {
	p.rwlock.Lock()
	defer p.rwlock.Unlock()

	var branchPage *InternalBranchPage
	var context *InternalDataContext

	//branchRootKey, branchKey := p._GetBranchKeys(pid)
	branchRootKey, branchKey := p._GetBranchKeys(pid)

	branchPage = p._GetOrCreateBranchPageByKey(branchRootKey)
	
	contextPageId, ok := branchPage.contextPageIdByBranchKey[branchKey]
	if !ok {
		contextPageId = p.pager.CreatePageId()
		branchPage.contextPageIdByBranchKey[branchKey] = contextPageId
		branchPage.isChanged = true

		context = _NewInternalDataContext(contextPageId)
		context.isChanged = true		

		p.contextByPageId[contextPageId] = context

		/*
		fmt.Println("---------------------------------")
		fmt.Println("---------------------------------")
		fmt.Println("INTERNAL CreatePageId", contextPageId)
		fmt.Println("---------------------------------")
		*/
	} 

	context, ok = p.contextByPageId[contextPageId]
	if !ok {
		context = p._ReadDataContext(contextPageId)
		p.contextByPageId[context.pid] = context	
	}

	context.dataByPageId[pid] = data
	context.isChanged = true
}


func (p *InternalPager) _GetOrCreateBranchPageByKey(branchRootKey uint32) *InternalBranchPage {

	branchPageId, ok := p.root[branchRootKey]
	if !ok {		

		branchPageId = p.pager.CreatePageId()
		p.root[branchRootKey] = branchPageId
		p.isChanged = true

		branchPage := _NewInternalBranchPage(branchPageId)		
		branchPage.isChanged = true

		p.branchPages[branchPage.pid] = branchPage
	}	

	return p._GetBranchPageByPageId(branchPageId)
}



func (p *InternalPager) _GetBranchPageByPageId(pid uint32) *InternalBranchPage {

	branchPage, ok := p.branchPages[pid]

	if !ok {
		branchPage = _NewInternalBranchPage(pid)

		data, ok := p.pager.ReadPayloadData(pid)

		if ok == nil {
			rd := NewDataStreamFromBuffer(data)
			var branchKey uint32
			var contextPageId uint32
			rowsCount := int(rd.ReadUInt32())
			for i:=0; i<rowsCount; i++ {
				branchKey = rd.ReadUInt32()
				contextPageId = rd.ReadUInt32()
				branchPage.contextPageIdByBranchKey[branchKey] = contextPageId
			}
		}

		p.branchPages[pid] = branchPage
	}

	return branchPage
}

func (p *InternalPager) Save() []byte {

	p.freelist.Save()

	fmt.Println("[InternalPager SAVE] freelist", len(p.freelist.pageIdSet))
	
	for _, branchPage := range p.branchPages {
		if true {
		//if branchPage.isChanged {

			w := NewDataStream()

			//fmt.Println(">> [SAVE]", branchPage.ToString(), branchPage.contextPageIdByBranchKey)

			w.WriteUInt32(uint32(len(branchPage.contextPageIdByBranchKey)))
			for branchKey, ctxPageId := range branchPage.contextPageIdByBranchKey {
				w.WriteUInt32(branchKey)
				w.WriteUInt32(ctxPageId)
			}
			p.pager.WritePayloadData(branchPage.pid, w.ToBytes())

			branchPage.isChanged = false
		}
	}
	


	//fmt.Println("[SAVE contextByPageId]", p.contextByPageId)

	for _, context := range p.contextByPageId {
		if true {
		//if context.isChanged {
			w := NewDataStream()

			w.WriteUInt32(uint32(len(context.dataByPageId)))
			for pageId, pageData := range context.dataByPageId {
				w.WriteUInt32(pageId)
				w.WriteChunk(pageData)
			}
			payload := w.ToBytes()
	//fmt.Println("--------------------------------", context.ToString())
			//fmt.Println("[SAVE CONTEXT]", "pid", ctxPid, "bytes", len(payload), "rows", len(context.dataByPageId))
			p.pager.WritePayloadData(context.pid, payload)

			context.isChanged = false
		}
	}

	if true {
	//if p.isChanged {
		w := NewDataStream()
		w.WriteUInt32(uint32(len(p.root)))

		for bkey, bpid := range p.root {
			w.WriteUInt32(bkey)
			w.WriteUInt32(bpid)
		}

		p.pager.WritePayloadData(p.rootPageId, w.ToBytes())

		p.isChanged = false
	}

	metaW := NewDataStreamFromBuffer(make([]byte, INTERNAL_PAGER_META_SIZE))
	
	metaW.WriteUInt16(p.pageSize)
	metaW.WriteUInt32(p.lastPageId)
	metaW.WriteUInt32(p.rootPageId)
	metaW.WriteUInt32(p.freelistPageId)

	return metaW.ToBytes()
}



func _NewInternalDataContext(pid uint32) *InternalDataContext {
	context := new(InternalDataContext)
	context.pid = pid
	context.dataByPageId = make(map[uint32][]byte)
	context.isChanged = true

	return context
}

func _NewInternalBranchPage(pid uint32) *InternalBranchPage {
	branchPage := new(InternalBranchPage)
	branchPage.pid = pid
	branchPage.contextPageIdByBranchKey = make(map[uint32]uint32)
	branchPage.isChanged = false

	return branchPage
}

func (bh *InternalBranchPage) ToString() string {
	return fmt.Sprintf("<InternalBranchPage pid=%v>", bh.pid)
}

func (ctx *InternalDataContext) ToString() string {
	return fmt.Sprintf("<InternalDataContext pid=%v>", ctx.pid)
}

func (p *InternalPager) _ReadDataContext(pid uint32) *InternalDataContext {

	//fmt.Println("")
	//fmt.Println("----------------------------------")
	
	contextPageData, err := p.pager.ReadPayloadData(pid)
	//fmt.Println("_ReadDataContext >>>>>>>>---- pid=", pid, "err", err, len(contextPageData))
	if err != nil {
		fmt.Println("err != nil", err)
		os.Exit(1)
	}

	context := _NewInternalDataContext(pid)

	if err == nil {
		rd := NewDataStreamFromBuffer(contextPageData)
		rowsCount := int(rd.ReadUInt32())
		
		for i:=0; i<rowsCount; i++ {
			_pid := rd.ReadUInt32()
			_data := rd.ReadChunk()
			context.dataByPageId[_pid] = _data

			//fmt.Println("[_ReadDataContext]", "_pid", _pid, "bytes", len(_data))
		}			
	}
	return context
}
