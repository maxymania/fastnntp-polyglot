/*
Copyright (c) 2019 Simon Schmidt

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

/*
PostgreSQL + Badger for storage.
*/
package pgbadge

import (
	"github.com/maxymania/fastnntp-polyglot"
	"github.com/maxymania/fastnntp-polyglot/gold"
	"github.com/maxymania/fastnntp-polyglot/buffer"
	"github.com/dgraph-io/badger"
	"github.com/byte-mug/golibs/msgpackx"
)


func flattenP(o *newspolyglot.ArticleOverview) []interface{} {
	return []interface{}{ &o.Subject,&o.From,&o.Date,&o.MsgId,&o.Refs,&o.Bytes,&o.Lines }
}
func flattenV(o *newspolyglot.ArticleOverview) []interface{} {
	return []interface{}{  o.Subject, o.From, o.Date, o.MsgId, o.Refs, o.Bytes, o.Lines }
}

func Initialize(q IQueryable) {
	q.Exec(`
	CREATE TABLE msgkhbo (
		msgid bytea PRIMARY KEY,
		khead bytea,
		kbody bytea,
		kover bytea,
		kttl  bigint
	)
	`)
	q.Exec(`
	CREATE INDEX msgkhbo_kttlbrin ON msgkhbo USING brin (kttl)
	`)
	q.Prepare("msgkhbo_maintain",`
	DELETE FROM msgkhbo WHERE kttl <= $1
	`)
	q.Prepare("msgkhbo_get0",`
	SELECT khead,kbody,kover FROM msgkhbo WHERE msgid=$1
	`)
	q.Prepare("msgkhbo_get1",`
	SELECT khead,kbody FROM msgkhbo WHERE msgid=$1
	`)
	q.Prepare("msgkhbo_get2",`
	SELECT kover FROM msgkhbo WHERE msgid=$1
	`)
	q.Prepare("msgkhbo_insert",`
	INSERT INTO msgkhbo (msgid,khead,kbody,kover,kttl) VALUES ($1,$2,$3,$4,$5)
	`)
}

func Maintainance(q IQueryable,current uint64) {
	q.Exec("msgkhbo_maintain",current)
}

func entry(k,v []byte,exp uint64) *badger.Entry {
	return &badger.Entry{
		Key: k,
		Value: v,
		ExpiresAt: exp,
	}
}
func putinto(pdst **[]byte,dst *[]byte,val []byte) {
	lng := len(val)
	buf := buffer.Get(lng)
	taa := (*buf)[:lng]
	copy(taa,val)
	*pdst = buf
	*dst = taa
}

type CStuff struct {
	Q IQueryable
	DB *badger.DB
}
func (i *CStuff) deleta(ks ...[]byte) {
	wb := i.DB.NewWriteBatch()
	for _,k := range ks { wb.Delete(k) }
	wb.Flush()
}
func (i *CStuff) ArticleDirectStat(id []byte) bool {
	var k1 []byte
	return i.Q.QueryRow("msgkhbo_get2",id).Scan(&k1)==nil
}
func (i *CStuff) ArticleDirectGet(id []byte, head, body bool) *newspolyglot.ArticleObject {
	var k1,k2 []byte
	err := i.Q.QueryRow("msgkhbo_get1",id).Scan(&k1,&k2)
	if err!=nil { return nil }
	
	obj := newspolyglot.AcquireArticleObject()
	ok := true
	txn := i.DB.NewTransaction(false)
	defer txn.Discard()
	if ok && head {
		ok = false
		if it,err := txn.Get(k1); err==nil {
			it.Value(func(val []byte) error{
				putinto(&(obj.Bufs[0]),&obj.Head, val)
				ok = true
				return nil
			})
		}
	}
	if ok && body {
		ok = false
		if it,err := txn.Get(k2); err==nil {
			it.Value(func(val []byte) error{
				putinto(&(obj.Bufs[1]),&obj.Body, val)
				ok = true
				return nil
			})
		}
	}
	if obj.Bufs[0]==nil { obj.Bufs[0],obj.Bufs[1] = obj.Bufs[1],nil }
	
	if !ok {
		buffer.Put(obj.Bufs[0])
		buffer.Put(obj.Bufs[1])
		newspolyglot.ReleaseArticleObject(obj)
		obj = nil
	}
	
	return obj
}
func (i *CStuff) ArticleDirectOverview(id []byte) *newspolyglot.ArticleOverview {
	var k1 []byte
	err := i.Q.QueryRow("msgkhbo_get2",id).Scan(&k1)
	if err!=nil { return nil }
	txn := i.DB.NewTransaction(false)
	defer txn.Discard()
	it,err := txn.Get(k1)
	if err!=nil { return nil }
	
	ov := newspolyglot.AcquireArticleOverview()
	
	err = it.Value(func(val []byte) error{
		return msgpackx.Unmarshal(val,flattenP(ov)...)
	})
	if err!=nil {
		newspolyglot.ReleaseArticleOverview(ov)
		return nil
	}
	
	return ov
}
func (i *CStuff) ArticleDirectStore(exp uint64, ov *newspolyglot.ArticleOverview, obj *newspolyglot.ArticleObject) (err error) {
	var over []byte
	over,err = msgpackx.Marshal(flattenV(ov)...)
	if err!=nil { return }
	
	k1,k2,k3 := generate3(ov.MsgId)
	
	wb := i.DB.NewWriteBatch()
	wb.SetEntry(entry(k1,over,exp))
	wb.SetEntry(entry(k2,obj.Head,exp))
	wb.SetEntry(entry(k3,obj.Body,exp))
	err = wb.Flush()
	if err!=nil { return err }
	
	_,err = i.Q.Exec("msgkhbo_insert",ov.MsgId,k2,k3,k1,exp)
	if err!=nil {
		i.deleta(k1,k2,k3)
		return err
	}
	
	return nil
}
func (i *CStuff) ArticleDirectRollback(id []byte) {
	var k1,k2,k3 []byte
	err := i.Q.QueryRow("msgkhbo_get0",id).Scan(&k1,&k2,&k3)
	if err==nil {
		i.deleta(k1,k2,k3)
	}
}

var _ gold.ArticleDirectEX = (*CStuff)(nil)

