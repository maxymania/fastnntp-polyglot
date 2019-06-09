/*
MIT License

Copyright (c) 2018 Simon Schmidt

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

package blue

import rb "github.com/emirpasic/gods/trees/redblacktree"
import "github.com/emirpasic/gods/utils"
import "github.com/gocql/gocql"
import "sync"
//import "sort"

type Request struct{
	IsRollback bool
	Group []byte
	Number uint64
	Err error
	WG sync.WaitGroup
}

type perGroup struct{
	// Init from outside
	session *gocql.Session
	group []byte
	
	// Init from inside
	seqs *rb.Tree
	ctr  uint64
	stream chan *Request
	rb,al []*Request
	buf1  []uint64
}
func (p *perGroup) init() (err error) {
	p.seqs = rb.NewWith(utils.UInt64Comparator)
	p.stream = make(chan *Request,1024)
	p.rb = make([]*Request,0,1024)
	p.al = make([]*Request,0,1024)
	p.buf1 = make([]uint64,0,1024)
	
	var fl []uint64
	var c uint64
	iter := p.session.Query(`
	SELECT freelst,headgrp FROM grouphead WHERE groupname=?
	`,p.group).Iter()
	iter.Scan(&fl,&c)
	err = iter.Close()
	if err!=nil { return }
	if c==0 { c = 1 }
	p.ctr = c
	for _,e := range fl { p.seqs.Put(e,nil) }
	
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
			} else if i<1024 {
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
	// Step 1: Fullfill the requests.
	iter := p.seqs.Iterator()
	nkeys := p.buf1
	rbi,rbn := 0,len(rb)
	updctr := false
	lctr := p.ctr
	i,n := 0,len(al)
	for ; (i<n)&&(rbi<rbn) ; i++ {
		req := al[i]
		req.Number = rb[rbi].Number
		rbi++
	}
	for ; (i<n)&&iter.Next() ; i++ {
		req := al[i]
		key := iter.Key().(uint64)
		req.Number = key
		nkeys = append(nkeys,key)
	}
	for ; i<n ; i++ {
		req := al[i]
		updctr = true
		req.Number = lctr
		lctr++
	}
	
	// Step 2: Apply changes to cassandra.
	var err error
	if rbi<rbn {
		lst := make([]uint64,rbn-rbi)
		for i,req := range rb[rbi:] {
			lst[i] = req.Number
		}
		err = p.session.Query(`
		UPDATE grouphead SET freelst = freelst + ? WHERE groupname = ?
		`,lst,p.group).Exec()
	} else if len(nkeys)>0 {
		if updctr {
			err = p.session.Query(`
			UPDATE grouphead SET freelst = freelst - ?,headgrp = ? WHERE groupname = ?
			`,nkeys,lctr,p.group).Exec()
		} else {
			err = p.session.Query(`
			UPDATE grouphead SET freelst = freelst - ? WHERE groupname = ?
			`,nkeys,p.group).Exec()
		}
	} else if updctr {
		err = p.session.Query(`
		UPDATE grouphead SET headgrp = ? WHERE groupname = ?
		`,lctr,p.group).Exec()
	} else {
		err = nil
	}
	
	// Step 3: Commit or roll back.
	if err==nil {
		// Remove allocated numbers from the free-list.
		for _,key := range nkeys { p.seqs.Remove(key) }
		// put released numbers to the free-list.
		for _,req := range rb[rbi:] { p.seqs.Put(req.Number,nil) }
		p.ctr = lctr
	} else {
		for i := range rb { rb[i].Err = err }
		for i := range al { al[i].Err = err }
	}
	for i := range rb { rb[i].WG.Done() }
	for i := range al { al[i].WG.Done() }
}

type global struct{
	sync.RWMutex
	
	// Init from outside
	session *gocql.Session
	
	// Init from inside
	groups map[string]*perGroup
}
func (g *global) init() {
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
		pgr = &perGroup{session:g.session,group:gc}
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

type Requester interface {
	Offer(p *Request) (err error)
}
func NewRequester(s *gocql.Session) Requester {
	g := &global{session:s}
	g.init()
	return g
}

