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

import "github.com/byte-mug/fastnntp"
import "github.com/vmihailenco/msgpack"
import "github.com/boltdb/bolt"


func (a *articleTransaction) ListGroups(wm *fastnntp.WildMat, ila fastnntp.IListActive) bool {
	var gi groupInfo
	c1 := a.tx.Bucket(tGRPNUMS).Cursor()
	gdescr := a.tx.Bucket(tGRPINFO)
	k,v := c1.First()
	for ; len(k)>0 ; k,v = c1.Next() {
		if msgpack.Unmarshal(v,&gi)!=nil { continue }
		ila.WriteFullInfo(k, gi[2], gi[1], byte(gi[3]), gdescr.Get(k))
		
	}
	return true
}

func (a *Wrapper) GroupStaticList(targ func(group []byte, descr []byte)) (ok bool) {
	a.DB.View(func(tx *bolt.Tx) (no error) {
		c1 := tx.Bucket(tGRPINFO).Cursor()
		k,v := c1.First()
		ok = true
		for ; len(k)>0 ; k,v = c1.Next() {
			targ(k, v)
		}
		return
	})
	return
}
func (a *Wrapper) GroupRealtimeList(targ func(group []byte, high, low int64, status byte)) (ok bool) {
	a.DB.View(func(tx *bolt.Tx) (no error) {
		var gi groupInfo
		c1 := tx.Bucket(tGRPNUMS).Cursor()
		k,v := c1.First()
		ok = true
		for ; len(k)>0 ; k,v = c1.Next() {
			if msgpack.Unmarshal(v,&gi)!=nil { continue }
			targ(k, gi[2], gi[1], byte(gi[3]))
		}
		return
	})
	return
}

