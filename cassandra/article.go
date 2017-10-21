/*
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


package cassandra

import "github.com/gocassa/gocassa"
import "github.com/maxymania/fastnntp-polyglot"
import "github.com/maxymania/fastnntp-polyglot/buffer"
import "github.com/byte-mug/fastnntp/posting"
import "github.com/davecgh/go-xdr/xdr2"
import "bytes"
import "github.com/maxymania/fastnntp-polyglot/policies"
import "time"

type StoredArticle struct{
	MsgId,Head,Body,Over []byte
	ExpireAt time.Time
}

func NewStoredArticleTable(ks gocassa.KeySpace) gocassa.Table {
	return ks.Table("v2articles",&StoredArticle{},gocassa.Keys{
		PartitionKeys: []string{"MsgId"},
	}).WithOptions(gocassa.Options{
		TableName: "v2articles", // Yes, We override the Table name.
	})
}

type ArticleStorage struct{
	ks     gocassa.KeySpace
	gnv    gocassa.Table
	sat    gocassa.Table
	Policy policies.PostingPolicy
}

func NewArticleStorage(ks gocassa.KeySpace) *ArticleStorage {
	a := new(ArticleStorage)
	a.ks  = ks
	a.gnv = GnvTable(ks)
	a.sat = NewStoredArticleTable(ks)
	return a
}
func (a *ArticleStorage) Initialize() {
	a.gnv.CreateIfNotExist()
	a.sat.CreateIfNotExist()
}

func (a *ArticleStorage) ArticleDirectStat(id []byte) bool {
	sat := []StoredArticle{}
	ok := a.sat.Where(gocassa.Eq("MsgId",id)).Read(sat).Run()!=nil
	return ok&&len(sat)!=0
}
func (a *ArticleStorage) ArticleDirectGet(id []byte, head, body bool) *newspolyglot.ArticleObject {
	var e error
	sat := new(StoredArticle)
	if a.sat.Where(gocassa.Eq("MsgId",id)).ReadOne(sat).Run()!=nil { return nil }
	obj := new(newspolyglot.ArticleObject)
	obj.Bufs[0],obj.Head,e = zdecode(sat.Head)
	if e!=nil { return nil }
	obj.Bufs[1],obj.Body,e = zdecode(sat.Body)
	if e!=nil {
		buffer.Put(obj.Bufs[0])
		return nil
	}
	return obj
}
func (a *ArticleStorage) ArticleDirectOverview(id []byte) *newspolyglot.ArticleOverview {
	sat := new(StoredArticle)
	if a.sat.Where(gocassa.Eq("MsgId",id)).ReadOne(sat).Run()!=nil { return nil }
	aov := new(newspolyglot.ArticleOverview)
	zunmarshal(sat.Over,aov) // Assume the correct format.
	return aov
}

func (a *ArticleStorage) ArticleGroupStat(group []byte, num int64, id_buf []byte) ([]byte, bool) {
	gnvo := new(GroupNumValue)
	if getGNV(a.gnv,group,num).ReadOne(&gnvo).Run()!=nil { return nil,false }
	aov := new(newspolyglot.ArticleOverview)
	zunmarshal(gnvo.Value,aov) // Assume the correct format.
	return aov.MsgId,true
}
func (a *ArticleStorage) ArticleGroupGet(group []byte, num int64, head, body bool, id_buf []byte) ([]byte, *newspolyglot.ArticleObject) {
	id,ok := a.ArticleGroupStat(group,num,id_buf)
	if !ok { return nil,nil }
	obj := a.ArticleDirectGet(id,head,body)
	if obj==nil { return nil,nil }
	return id,obj
}
func (a *ArticleStorage) ArticleGroupOverview(group []byte, first, last int64, targ func(int64, *newspolyglot.ArticleOverview)) {
	aov := new(newspolyglot.ArticleOverview)
	getLoopGNV(a.gnv,group,first,last,func(i int64,b []byte) bool {
		zunmarshal(b,aov) // Assume the correct format.
		targ(i,aov)
		return true
	})
}
func (a *ArticleStorage) ArticleGroupList(group []byte, first, last int64, targ func(int64)) {
	getLoopGNV(a.gnv,group,first,last,func(i int64,b []byte) bool {
		targ(i)
		return true
	})
	
}
func (a *ArticleStorage) ArticleGroupMove(group []byte, i int64, backward bool, id_buf []byte) (ni int64, id []byte, ok bool) {
	buf := []byte(nil)
	if backward {
		ni,buf,ok = getPrevGNV(a.sat,group,i)
	} else {
		ni,buf,ok = getNextGNV(a.sat,group,i)
	}
	if ok {
		aov := new(newspolyglot.ArticleOverview)
		zunmarshal(buf,aov) // Assume the correct format.
		id = aov.MsgId
	}
	return
}

func (a *ArticleStorage) ArticlePostingPost(headp *posting.HeadInfo, body []byte, ngs [][]byte, numbs []int64) (rejected bool, failed bool, err error) {
	ao := new(newspolyglot.ArticleOverview)
	ao.Subject = headp.Subject
	ao.From    = headp.From
	ao.Date    = headp.Date
	ao.MsgId   = headp.MessageId
	ao.Refs    = headp.References
	ao.Bytes   = int64(len(headp.RAW)+2+len(body))
	ao.Lines   = posting.CountLines(body)
	
	decision := policies.Def(a.Policy).Decide(ngs,ao.Lines,ao.Bytes)
	
	chunk := []byte(nil)
	{
		buf := new(bytes.Buffer)
		xdr.Marshal(buf,&(ao))
		chunk = buf.Bytes()
	}
	chunk = decision.CompressXover.Def()(policies.DEFLATE{},chunk)
	
	art := new(StoredArticle)
	art.MsgId    = headp.MessageId
	art.Head     = decision.CompressHeader.Def()(policies.DEFLATE{},headp.RAW)
	art.Body     = decision.CompressBody.Def()(policies.DEFLATE{},body)
	art.Over     = chunk
	art.ExpireAt = decision.ExpireAt
	
	op := a.sat.Set(art)
	for i,ng := range ngs {
		op = op.Add(a.gnv.Set(mkGNV(ng,numbs[i],chunk,decision.ExpireAt)))
	}
	if a.ArticleDirectStat(headp.MessageId) { return true,false,nil } // We already have it.
	if op.Run()!=nil { return false,true,nil }
	return false,false,nil
}
func (a *ArticleStorage) ArticlePostingCheckPost() (possible bool) {
	return true
}
func (a *ArticleStorage) ArticlePostingCheckPostId(id []byte) (wanted bool, possible bool) {
	if a.ArticleDirectStat(id) { return false,true }
	return true,true
}

