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

import "github.com/vmihailenco/msgpack"
import "github.com/boltdb/bolt"

type groupInfo [4]int64 /* [ Count, Low, High, Status ]*/


func (a *Wrapper) GroupRealtimeQuery(group []byte) (number int64,low int64,high int64,ok bool) {
	a.DB.View(func(tx *bolt.Tx) (no error) {
		var gi groupInfo
		b := tx.Bucket(tGRPNUMS).Get(group)
		if len(b)==0 { return }
		e := msgpack.Unmarshal(b,&gi)
		ok = e==nil
		number = gi[0]
		low    = gi[1]
		high   = gi[2]
		return
	})
	return
}


func (a *Wrapper) ArticleGroupList(group []byte, first, last int64,targ func(int64)) {
	t,e := a.begin(false)
	if e!=nil { return }
	defer t.commit()
	t.ArticleGroupList(group,first,last,targ)
}
func (a *articleTransaction) ArticleGroupList(group []byte, first, last int64,targ func(int64)) {
	bkt := a.tx.Bucket(tGRPARTS).Bucket(group)
	if bkt==nil { return }
	c := bkt.Cursor()
	k,_ := c.Seek(encode64(first))
	for len(k)>0 {
		num := decode64(k)
		if num>last { break }
		targ(num)
		k,_ = c.Next()
	}
}

func (a *Wrapper) ArticleGroupMove(group []byte, i int64, backward bool, id_buf []byte) (ni int64, id []byte, ok bool) {
	t,e := a.begin(false)
	if e!=nil { ok = false; return }
	defer t.commit()
	return t.ArticleGroupMove(group,i,backward,id_buf)
}
func (a *articleTransaction) ArticleGroupMove(group []byte, i int64, backward bool, id_buf []byte) (ni int64, id []byte, ok bool) {
	bkt := a.tx.Bucket(tGRPARTS).Bucket(group)
	if bkt==nil { return }
	c := bkt.Cursor()
	k,v := c.Seek(encode64(i))
	if len(k)==0 { return }
	if backward { k,v = c.Prev() } else { k,v = c.Next() }
	if len(k)==0 { return }
	
	ni = decode64(k)
	id = append(id_buf,v...)
	ok = true
	return
}
