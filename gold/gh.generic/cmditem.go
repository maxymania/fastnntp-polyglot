/*
MIT License

Copyright (c) 2018-2020 Simon Schmidt

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

package generic

import "sync"

type BulkAllocator interface{
	AllocIds(group []byte,buf []uint64) ([]uint64,error)
	RevertIds(group []byte,buf []uint64) (error)
}


type perGroup struct{
	// Init from outside
	session BulkAllocator
	group []byte
	rpgsize int // Requests Per Group Size
	
	// Init from inside
	stream chan *Request
	rb,al []*Request
	buf1  []uint64
}
func (p *perGroup) init() (err error) {
	p.stream = make(chan *Request,p.rpgsize)
	p.rb = make([]*Request,0,p.rpgsize)
	p.al = make([]*Request,0,p.rpgsize)
	p.buf1 = make([]uint64,0,p.rpgsize)
	
	go p.reqloop()
	return
}
func (p *perGroup) reqloop() {
	for {
		i := 0
		rb := p.rb
		al := p.al
		for {
			var r *Request
			if i==0 {
				r = <- p.stream
			} else if i<p.rpgsize {
				select {
				case r = <- p.stream:
				default: goto done
				}
			} else { goto done }
			i++
			if r.IsRollback {
				rb = append(rb,r)
			} else {
				al = append(al,r)
			}
		}
	done:
		p.perform(rb,al)
	}
}
func (p *perGroup) perform(rb,al []*Request) {
	// Step 1: Shortcut the ids.
	nkeys := p.buf1
	rbi,rbn := 0,len(rb)
	i,n := 0,len(al)
	for ; (i<n)&&(rbi<rbn) ; i++ {
		req := al[i]
		rbo := rb[rbi]
		req.Number = rbo.Number
		req.WG.Done()
		rbo.WG.Done()
		rbi++
	}
	al = al[i:]
	rb = rb[rbi:]
	
	// Step 2: Apply changes to cassandra.
	for len(al)>0 {
		keys,err := p.session.AllocIds(p.group,nkeys[:len(al)])
		if err!=nil {
			for _,req := range al {
				req.Err = err
				req.WG.Done()
			}
			break
		}
		for j,id := range keys {
			req := al[j]
			req.Number = id
			req.WG.Done()
		}
		al = al[len(keys):]
	}
	if len(rb)>0 {
		for j,rbo := range rb {
			nkeys[j] = rbo.Number
		}
		err := p.session.RevertIds(p.group,nkeys[:len(rb)])
		for _,rbo := range rb {
			rbo.Err = err
			rbo.WG.Done()
		}
	}
}

type global struct{
	sync.RWMutex
	
	// Init from outside
	session BulkAllocator
	rpgsize int // Requests Per Group Size
	
	// Init from inside
	groups map[string]*perGroup
}
func (g *global) init() {
	if g.rpgsize<1 { g.rpgsize = 128 }
	g.groups = make(map[string]*perGroup)
}
func (g *global) lookup(grp []byte) *perGroup {
	g.RLock(); defer g.RUnlock()
	return g.groups[string(grp)]
}
func (g *global) instantiate(grp []byte) (pgr *perGroup,err error) {
	g.Lock(); defer g.Unlock()
	pgr = g.groups[string(grp)]
	if pgr==nil {
		gc := make([]byte,len(grp))
		copy(gc,grp)
		pgr = &perGroup{session:g.session,group:gc,rpgsize:g.rpgsize}
		err = pgr.init()
		if err==nil {
			g.groups[string(gc)] = pgr
		}
	}
	return
}
func (g *global) Offer(p *Request) (err error) {
	pgr := g.lookup(p.Group)
	if pgr==nil {
		pgr,err = g.instantiate(p.Group)
	}
	if err!=nil { return }
	pgr.stream <- p
	return
}

func NewRequesterSimple(s BulkAllocator) Requester {
	g := &global{session:s}
	g.init()
	return g
}
func NewRequester(s BulkAllocator,requestsPerGroup int) Requester {
	g := &global{session:s,rpgsize:requestsPerGroup}
	g.init()
	return g
}
