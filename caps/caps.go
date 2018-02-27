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


package caps

import "github.com/byte-mug/fastnntp"
import "github.com/byte-mug/fastnntp/posting"
import "github.com/maxymania/fastnntp-polyglot"
import "github.com/maxymania/fastnntp-polyglot/buffer"
import "bytes"
import "fmt"

import "sync"

var pvArticleWriter,pvXoverWriter,pvXoverWriter2,pvNumberPrinter,pvGroupLister sync.Pool

func Dedupe(names [][]byte) [][]byte {
	i := 0
	for _,name := range names{
		for j:=0 ; j<i ; i++ {
			if bytes.Equal(names[i],name) { goto next }
		}
		names[i] = name
		i++
		next:
	}
	return names[:i]
}

func lam2bool(lam fastnntp.ListActiveMode) (active bool, descr bool) {
	switch lam {
	case fastnntp.LAM_Full: active = true; descr = true
	case fastnntp.LAM_Active: active = true;
	case fastnntp.LAM_Newsgroups: descr = true
	}
	return
}

type Caps struct{
	Stamper posting.Stamper
	GroupHeadDB newspolyglot.GroupHeadDB
	GroupHeadCache newspolyglot.GroupHeadCache
	ArticlePostingDB newspolyglot.ArticlePostingDB
	ArticleDirectDB newspolyglot.ArticleDirectDB
	ArticleGroupDB newspolyglot.ArticleGroupDB
	GroupRealtimeDB newspolyglot.GroupRealtimeDB
	GroupStaticDB newspolyglot.GroupStaticDB
}


func (a *Caps) CheckPost() (possible bool) { return a.ArticlePostingDB.ArticlePostingCheckPost() }
func (a *Caps) CheckPostId(id []byte) (wanted bool, possible bool) {
	return a.ArticlePostingDB.ArticlePostingCheckPostId(id)
}

func (a *Caps) PerformPost(id []byte, r *fastnntp.DotReader) (rejected bool, failed bool) {
	head,body := posting.ConsumePostedArticle(r)
	if len(head)==0 || len(body)==0 { return false,true }
	
	headp := posting.ParseAndProcessHeader(id,a.Stamper,head)
	if headp==nil { return true,false }
	
	if len(headp.MessageId)==0 { return false,true } // no message-ID? Failed.
	
	ngrps := posting.SplitNewsgroups(headp.Newsgroups)
	if len(ngrps)==0 { return true,false }
	ngrps = Dedupe(ngrps)
	
	ngrps,e := a.GroupHeadCache.GroupHeadFilter(ngrps)
	if e!=nil { return false,true }
	if len(ngrps)==0 { return true,false }
	
	nums,e := a.GroupHeadDB.GroupHeadInsert(ngrps,nil)
	if e!=nil { return false,true }
	
	rej,fl,e := a.ArticlePostingDB.ArticlePostingPost(headp,body,ngrps,nums)
	if e!=nil { fl = true }
	
	if rej||fl {
		a.GroupHeadDB.GroupHeadRevert(ngrps,nums)
	}
	return rej,fl
}

func (a *Caps) StatArticle(ar *fastnntp.Article) bool {
	if ar.HasId {
		return a.ArticleDirectDB.ArticleDirectStat(ar.MessageId)
	}
	if ar.HasNum {
		id,ok := a.ArticleGroupDB.ArticleGroupStat(ar.Group, ar.Number,ar.MessageId)
		if ok { ar.MessageId = id }
		return ok
	}
	return false
}

func (a *Caps) GetArticle(ar *fastnntp.Article, head, body bool) func(w *fastnntp.DotWriter) {
	if ar.HasId {
		article := a.ArticleDirectDB.ArticleDirectGet(ar.MessageId,head,body)
		if article!=nil {
			return mkArticleWriter(article,head,body).wObject
		}
		return nil
	}
	if ar.HasNum {
		id,article := a.ArticleGroupDB.ArticleGroupGet(ar.Group,ar.Number,head,body,ar.MessageId)
		if article==nil { return nil }
		ar.MessageId = id
		ar.HasId = true
		return mkArticleWriter(article,head,body).wObject
	}
	return nil
}
func (a *Caps) WriteOverview(ar *fastnntp.ArticleRange) func(w fastnntp.IOverview) {
	if ar.HasId {
		artc := a.ArticleDirectDB.ArticleDirectOverview(ar.MessageId)
		if artc==nil { return nil }
		return mkXoverWriter(artc).wObject
	}
	if ar.HasNum {
		return mkXoverWriter2(a.ArticleGroupDB,ar).qObject
	}
	return nil
}

func (a *Caps) CursorMoveGroup(g *fastnntp.Group, i int64, backward bool, id_buf []byte) (ni int64, id []byte, ok bool) {
	return a.ArticleGroupDB.ArticleGroupMove(g.Group,i,backward,id_buf)
}
func (a *Caps) ListGroup(g *fastnntp.Group, w *fastnntp.DotWriter, first, last int64) {
	if first<g.Low { first = g.Low }
	if last>g.High { last = g.High }
	// Restrict the range.
	n := pvNumberPrinter.Get().(*numberPrinter)
	n.w = w
	defer n.free()
	a.ArticleGroupDB.ArticleGroupList(g.Group,first,last,n.nObject)
	return
}

type numberPrinter struct {
	w *fastnntp.DotWriter
	nObject func(num int64)
}
func gtNumberPrinter() interface{} {
	p := new(numberPrinter)
	p.nObject = p.number
	return p
}

func (n *numberPrinter) number(num int64) {
	fmt.Fprintf(n.w,"%v\r\n",num)
}
func (n *numberPrinter) free(){
	n.w = nil
	pvNumberPrinter.Put(n)
}

func (a *Caps) GetGroup(g *fastnntp.Group) bool {
	n,l,h,ok := a.GroupRealtimeDB.GroupRealtimeQuery(g.Group)
	if ok {
		g.Number = n
		g.Low    = l
		g.High   = h
	}
	return ok
}

type articleWriter struct{
	object *newspolyglot.ArticleObject
	head, body bool
	wObject func(w *fastnntp.DotWriter)
}

func gtArticleWriter() interface{} {
	aw := new(articleWriter)
	aw.wObject = aw.write
	return aw
}

func mkArticleWriter(object *newspolyglot.ArticleObject, head, body bool) *articleWriter {
	obj := pvArticleWriter.Get().(*articleWriter)
	obj.object = object
	obj.head   = head
	obj.body   = body
	return obj
}

func (aw *articleWriter) write(w *fastnntp.DotWriter) {
	if aw.head {
		w.Write(aw.object.Head)
	}
	if aw.head && aw.body { w.Write([]byte("\r\n")) }
	if aw.body {
		w.Write(aw.object.Body)
	}
	w.Write([]byte(".\r\n"))
	for _,buf := range aw.object.Bufs { buffer.Put(buf) }
	newspolyglot.ReleaseArticleObject(aw.object)
	aw.object = nil
	pvArticleWriter.Put(aw)
}
type xoverWriter struct{
	over *newspolyglot.ArticleOverview
	wObject func(w fastnntp.IOverview)
}
func (x *xoverWriter) write(w fastnntp.IOverview) {
	w.WriteEntry(0, x.over.Subject, x.over.From, x.over.Date, x.over.MsgId, x.over.Refs, x.over.Bytes, x.over.Lines)
	newspolyglot.ReleaseArticleOverview(x.over)
}

func gtXoverWriter() interface{} {
	x := new(xoverWriter)
	x.wObject = x.write
	return x
}
func mkXoverWriter(over *newspolyglot.ArticleOverview) *xoverWriter {
	x := pvXoverWriter.Get().(*xoverWriter)
	x.over = over
	return x
}

type xoverWriter2 struct{
	ag newspolyglot.ArticleGroupDB
	ar *fastnntp.ArticleRange
	aw fastnntp.IOverview
	
	qObject func(w fastnntp.IOverview)
	wObject func(num int64,xover *newspolyglot.ArticleOverview)
}
func gtXoverWriter2() interface{} {
	x := new(xoverWriter2)
	x.qObject = x.query
	x.wObject = x.write
	return x
}
func mkXoverWriter2(ag newspolyglot.ArticleGroupDB,ar *fastnntp.ArticleRange) *xoverWriter2 {
	x := pvXoverWriter2.Get().(*xoverWriter2)
	x.ag = ag
	x.ar = ar
	return x
}

func (x *xoverWriter2) query(w fastnntp.IOverview) {
	x.aw = w
	x.ag.ArticleGroupOverview(x.ar.Group,x.ar.Number,x.ar.LastNumber,x.wObject)
	x.ag = nil
	x.ar = nil
	x.aw = nil
	pvXoverWriter2.Put(x)
}
func (x *xoverWriter2) write(num int64,xover *newspolyglot.ArticleOverview) {
	x.aw.WriteEntry(num, xover.Subject, xover.From, xover.Date, xover.MsgId, xover.Refs, xover.Bytes, xover.Lines)
}

type groupLister struct{
	ila fastnntp.IListActive
	aObject func(group []byte, high, low int64, status byte)
	nObject func(group []byte, descr []byte)
}
func (g *groupLister) active(group []byte, high, low int64, status byte) {
	g.ila.WriteActive(group, high, low, status)
}
func (g *groupLister) newsgroup(group []byte, descr []byte) {
	g.ila.WriteNewsgroups(group, descr)
}
func (g *groupLister) free() {
	g.ila = nil
	pvGroupLister.Put(g)
}
func gtGroupLister() interface{} {
	g := new(groupLister)
	g.aObject = g.active
	g.nObject = g.newsgroup
	return g
}

func (a *Caps) ListGroups(wm *fastnntp.WildMat, ila fastnntp.IListActive) bool {
	gt := pvGroupLister.Get().(*groupLister)
	defer gt.free()
	gt.ila = ila
	active,descr := lam2bool(ila.GetListActiveMode())
	if !descr {
		return a.GroupRealtimeDB.GroupRealtimeList(gt.aObject)
	}
	if !active {
		return a.GroupStaticDB.GroupStaticList(gt.nObject)
	}
	return false
}


func init(){
	pvArticleWriter.New = gtArticleWriter
	pvXoverWriter.New = gtXoverWriter
	pvXoverWriter2.New = gtXoverWriter2
	pvNumberPrinter.New = gtNumberPrinter
	pvGroupLister.New = gtGroupLister
}
