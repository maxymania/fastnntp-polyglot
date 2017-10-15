/*
MIT License

Copyright (c) 2017 Simon Schmidt

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


package oldbolt

import "github.com/byte-mug/fastnntp/posting"
import "github.com/vmihailenco/msgpack"

import "github.com/boltdb/bolt"


func (a *articleTransaction) CheckPostId(id []byte) (wanted bool, possible bool) {
	v := a.tx.Bucket(tARTMETA).Get(id)
	if len(v)>0 { return false,true }
	return true,true
}

func (a *Wrapper) ArticlePostingCheckPostId(id []byte) (wanted bool, possible bool) {
	t,e := a.begin(false)
	if e!=nil { return true,false }
	defer t.commit()
	return t.CheckPostId(id)
}

func (a *Wrapper) ArticlePostingCheckPost() (possible bool) { return true }

func (a *Wrapper) GroupHeadFilter(groups [][]byte) ([][]byte,error) {
	t,e := a.begin(false)
	if e!=nil { return groups,e }
	defer t.commit()
	return t.filterNewsgroups(groups),nil
}

func (a *Wrapper) GroupHeadInsert(ngs [][]byte,buf []int64) ([]int64,error) {
	{
		l := len(ngs)
		if cap(buf)<l { buf = make([]int64,l) }
		buf = buf[:l]
	}
	
	err := a.DB.Update(func(tx *bolt.Tx) error {
		nums := tx.Bucket(tGRPNUMS)
		gi := new(groupInfo)
	
		for i,group := range ngs {
			v := nums.Get(group)
			if len(v)==0 { continue }
			if msgpack.Unmarshal(v,gi)!=nil { continue }
		
			gi[0]++ // Number
			if gi[1]==0 { gi[1] = 1 } // Low
			gi[2]++ // High
			buf[i] = gi[2]
		
			v,_ = msgpack.Marshal(gi)
			if err := nums.Put(group,v) ; err!=nil { return err } // Propagate error!
		}
		return nil
	})
	return buf,err
}
func (a *Wrapper) GroupHeadRevert(ngs [][]byte,numbs []int64) error {
	err := a.DB.Update(func(tx *bolt.Tx) error {
		nums := tx.Bucket(tGRPNUMS)
		gi := new(groupInfo)
	
		for i,group := range ngs {
			v := nums.Get(group)
			if len(v)==0 { continue }
			if msgpack.Unmarshal(v,gi)!=nil { continue }
		
			gi[0]-- // Number
			if gi[2] /* High */ == numbs[i] { gi[2]-- }
			
			if gi[1]<gi[2] { gi[1] = gi[2] }
		
			v,_ = msgpack.Marshal(gi)
			if err := nums.Put(group,v) ; err!=nil { return err } // Propagate error!
		}
		return nil
	})
	return err
}


func (a *articleTransaction) filterNewsgroups(ngs [][]byte) [][]byte {
	i := 0
	nums := a.tx.Bucket(tGRPNUMS)
	gi := new(groupInfo)
	
	for _,group := range ngs {
		v := nums.Get(group)
		if len(v)==0 { continue }
		if msgpack.Unmarshal(v,gi)!=nil { continue }
		if gi[3]!='y' { continue }
		ngs[i] = group
		i++
	}
	
	return ngs[:i]
}


func (a *articleTransaction) insertIntoGroups2(id []byte, am *articleMetadata, ngs [][]byte, numbs []int64) {
	arts := a.tx.Bucket(tGRPARTS)
	
	for i,group := range ngs {
		if _,ok := am.Nums[string(group)] ; ok { continue } // avoid duplicate insertion
		
		num := numbs[i]
		
		gbk := arts.Bucket(group)
		if gbk==nil { continue } // Can't update group
		if gbk.Put(encode64(num),id)!=nil { continue }
		
		am.Nums[string(group)] = num // Set number.
		am.Refc++
	}
}

func (a *Wrapper) ArticlePostingPost(headp *posting.HeadInfo,body []byte, ngs [][]byte, numbs []int64) (rejected bool, failed bool,err error) {
	t,e := a.begin(true)
	if e!=nil { return false,true,e }
	rejected,failed = t.performPost(headp,body,ngs,numbs)
	err = t.commit()
	return
}

func (a *articleTransaction) performPost(headp *posting.HeadInfo,body []byte, ngs [][]byte, numbs []int64) (rejected bool, failed bool) {
	am := &articleMetadata{ Nums: make(map[string]int64) }
	ao := &articleOver{}
	
	// Subject, From, Date, MsgId, Refs
	ao.SF[0] = headp.Subject
	ao.SF[1] = headp.From
	ao.SF[2] = headp.Date
	ao.SF[3] = headp.MessageId
	ao.SF[4] = headp.References
	
	// Bytes, Lines
	ao.LN[0] = int64(len(headp.RAW)+2+len(body))
	ao.LN[1] = posting.CountLines(body)
	
	aob,_ := msgpack.Marshal(ao)
	
	a.tx.Bucket(tARTOVER).Put(headp.MessageId,aob)
	a.tx.Bucket(tARTHEAD).Put(headp.MessageId,headp.RAW)
	a.tx.Bucket(tARTBODY).Put(headp.MessageId,body)
	
	a.insertIntoGroups2(headp.MessageId,am,ngs,numbs)
	if am.Refc==0 { a.rollback(); return true,false }
	
	amb,_ := msgpack.Marshal(am)
	a.tx.Bucket(tARTMETA).Put(headp.MessageId,amb)
	return
}

