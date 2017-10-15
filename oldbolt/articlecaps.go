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
import "github.com/maxymania/fastnntp-polyglot"
import "github.com/maxymania/fastnntp-polyglot/buffer"
import "github.com/boltdb/bolt"

type articleMetadata struct{
	Refc int64
	Nums map[string]int64
}

type articleOver struct{
	SF [5][]byte // Subject, From, Date, MsgId, Refs
	LN [2]int64  // Bytes, Lines
}

func (a *articleTransaction) StatArticle(ar *fastnntp.Article) bool {
	if ar.HasNum && !ar.HasId {
		bkt := a.tx.Bucket(tGRPARTS).Bucket(ar.Group)
		if bkt==nil { return false }
		v := bkt.Get(encode64(ar.Number))
		if len(v)==0 { return false }
		ar.MessageId = append(ar.MessageId[:0],v...)
		ar.HasId = true
		return true
	}
	if ar.HasId {
		if len(a.tx.Bucket(tARTMETA).Get(ar.MessageId))==0 { return false }
		return true
	}
	return false
}

func (a *Wrapper) ArticleDirectStat(id []byte) bool {
	ok := false
	err := a.DB.View(func(tx *bolt.Tx) error {
		ok = len(tx.Bucket(tARTMETA).Get(id))>0
		return nil
	})
	return ok && err==nil
}
func (a *Wrapper) ArticleDirectGet(id []byte,head ,body bool) *newspolyglot.ArticleObject {
	article := new(newspolyglot.ArticleObject)
	err := a.DB.View(func(tx *bolt.Tx) error {
		if head {
			h := tx.Bucket(tARTHEAD).Get(id)
			if len(h)==0 { article = nil ; return nil }
			article.Bufs[0] = buffer.Get(len(h))
			article.Head = append((*article.Bufs[0])[:0],h...)
		}
		if body {
			h := tx.Bucket(tARTBODY).Get(id)
			if len(h)==0 { article = nil ; return nil }
			article.Bufs[1] = buffer.Get(len(h))
			article.Body = append((*article.Bufs[1])[:0],h...)
		}
		return nil
	})
	if err!=nil { return article }
	return article
}

func (a *Wrapper) ArticleDirectOverview(id []byte) *newspolyglot.ArticleOverview {
	article := new(newspolyglot.ArticleOverview)
	err := a.DB.View(func(tx *bolt.Tx) error {
		var ao articleOver
		v := tx.Bucket(tARTOVER).Get(id)
		if err := msgpack.Unmarshal(v,&ao) ; err!=nil { return err }
		article.Subject = ao.SF[0]
		article.From    = ao.SF[1]
		article.Date    = ao.SF[2]
		article.MsgId   = ao.SF[3]
		article.Refs    = ao.SF[4]
		article.Bytes   = ao.LN[0]
		article.Lines   = ao.LN[1]
		return nil
	})
	if err!=nil { return article }
	return article
}


func (a *Wrapper) ArticleGroupStat(group []byte, num int64,id_buf []byte) (id []byte, dostat bool) {
	a.DB.View(func(tx *bolt.Tx) (no error) {
		bkt := tx.Bucket(tGRPARTS).Bucket(group)
		if bkt==nil { return  }
		v := bkt.Get(encode64(num))
		if len(v)==0 { return }
		id = append(id_buf[:0],v...)
		dostat = true
		return
	})
	return
}
func (a *Wrapper) ArticleGroupGet(group []byte, num int64,head ,body bool,id_buf []byte) (id []byte,article *newspolyglot.ArticleObject) {
	a.DB.View(func(tx *bolt.Tx) (no error) {
		bkt := tx.Bucket(tGRPARTS).Bucket(group)
		if bkt==nil { return  }
		v := bkt.Get(encode64(num))
		if len(v)==0 { return }
		id = append(id_buf[:0],v...)
		article = new(newspolyglot.ArticleObject)
		if head {
			h := tx.Bucket(tARTHEAD).Get(id)
			if len(h)==0 { article = nil ; return }
			article.Bufs[0] = buffer.Get(len(h))
			article.Head = append((*article.Bufs[0])[:0],h...)
		}
		if body {
			h := tx.Bucket(tARTBODY).Get(id)
			if len(h)==0 { article = nil ; return }
			article.Bufs[1] = buffer.Get(len(h))
			article.Body = append((*article.Bufs[1])[:0],h...)
		}
		return
	})
	return
}

func (a *Wrapper) ArticleGroupOverview(group []byte, first, last int64,targ func(*newspolyglot.ArticleOverview)) {
	article := new(newspolyglot.ArticleOverview)
	a.DB.View(func(tx *bolt.Tx) error {
		var ao articleOver
		aov := tx.Bucket(tARTOVER)
		bkt := tx.Bucket(tGRPARTS).Bucket(group)
		if bkt==nil { return nil }
		c := bkt.Cursor()
		k,m := c.Seek(encode64(first))
		for ; len(k)!=0 ; k,m = c.Next() {
			num := decode64(k)
			if num>last { break }
			v := aov.Get(m)
			if msgpack.Unmarshal(v,&ao)!=nil { continue }
			article.Subject = ao.SF[0]
			article.From    = ao.SF[1]
			article.Date    = ao.SF[2]
			article.MsgId   = ao.SF[3]
			article.Refs    = ao.SF[4]
			article.Bytes   = ao.LN[0]
			article.Lines   = ao.LN[1]
			targ(article)
		}
		return nil
	})
}


