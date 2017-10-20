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


package caps

import "github.com/byte-mug/fastnntp"
import "github.com/byte-mug/fastnntp/posting"
import "github.com/maxymania/fastnntp-polyglot"
import "github.com/maxymania/fastnntp-polyglot/buffer"
import "bytes"
import "fmt"

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
			return (&articleWriter{article,head,body}).write
		}
		return nil
	}
	if ar.HasNum {
		id,article := a.ArticleGroupDB.ArticleGroupGet(ar.Group,ar.Number,head,body,ar.MessageId)
		if article==nil { return nil }
		ar.MessageId = id
		ar.HasId = true
		return (&articleWriter{article,head,body}).write
	}
	return nil
}
func (a *Caps) WriteOverview(ar *fastnntp.ArticleRange) func(w fastnntp.IOverview) {
	if ar.HasId {
		artc := a.ArticleDirectDB.ArticleDirectOverview(ar.MessageId)
		return (&xoverWriter{artc}).write
	}
	if ar.HasNum {
		return func(w fastnntp.IOverview) {
			a.ArticleGroupDB.ArticleGroupOverview(ar.Group,ar.Number,ar.LastNumber,func(num int64,xover *newspolyglot.ArticleOverview) {
				w.WriteEntry(num, xover.Subject, xover.From, xover.Date, xover.MsgId, xover.Refs, xover.Bytes, xover.Lines)
			})
		}
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
	a.ArticleGroupDB.ArticleGroupList(g.Group,first,last,func(num int64){
		fmt.Fprintf(w,"%v\r\n",num)
	})
	return
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
}
func (aw *articleWriter) write(w *fastnntp.DotWriter) {
	if aw.head {
		w.Write(aw.object.Head)
	}
	if aw.head && aw.body { w.Write([]byte("\r\n")) }
	if aw.body {
		w.Write(aw.object.Body)
	}
	for _,buf := range aw.object.Bufs { buffer.Put(buf) }
	w.Write([]byte(".\r\n"))
}
type xoverWriter struct{
	over *newspolyglot.ArticleOverview
}
func (x *xoverWriter) write(w fastnntp.IOverview) {
	w.WriteEntry(0, x.over.Subject, x.over.From, x.over.Date, x.over.MsgId, x.over.Refs, x.over.Bytes, x.over.Lines)
}
func (a *Caps) ListGroups(wm *fastnntp.WildMat, ila fastnntp.IListActive) bool {
	active,descr := lam2bool(ila.GetListActiveMode())
	if !descr {
		return a.GroupRealtimeDB.GroupRealtimeList(func(group []byte, high, low int64, status byte){
			ila.WriteActive(group, high, low, status)
		})
	}
	if !active {
		return a.GroupStaticDB.GroupStaticList(func(group []byte, descr []byte){
			ila.WriteNewsgroups(group, descr)
		})
	}
	return false
}
