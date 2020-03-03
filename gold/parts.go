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
Mature implemenations of various backend-modules.
*/
package gold

import "github.com/maxymania/fastnntp-polyglot"
import "github.com/maxymania/fastnntp-polyglot/postauth"
import "github.com/byte-mug/fastnntp/posting"

type ArticleGroupEX interface {
	newspolyglot.ArticleGroupDB
	
	StoreArticleInfos(groups [][]byte, nums []int64, exp uint64, ov *newspolyglot.ArticleOverview) (err error)
	GroupRealtimeQuery(group []byte) (number int64, low int64, high int64, ok bool)
}

type ArticleDirectEX interface {
	newspolyglot.ArticleDirectDB
	
	ArticleDirectStore(exp uint64, ov *newspolyglot.ArticleOverview,obj *newspolyglot.ArticleObject) (err error)
	ArticleDirectRollback(id []byte)
}

type ArticleGroupWrapper struct {
	newspolyglot.ArticleGroupDB
	Direct newspolyglot.ArticleDirectDB
}
func (a *ArticleGroupWrapper) ArticleGroupGet(group []byte, num int64, head, body bool, id_buf []byte) ([]byte, *newspolyglot.ArticleObject) {
	id,ok := a.ArticleGroupStat(group,num,id_buf)
	if !ok { return nil,nil }
	ao := a.Direct.ArticleDirectGet(id,head,body)
	if ao==nil { return nil,nil }
	return id,ao
}

type GroupListDB interface {
	AddGroupDescr(group, descr []byte) error
	AddGroupStatus(group []byte, status byte) error
	
	GroupHeadFilterWithAuth(rank postauth.AuthRank, groups [][]byte) ([][]byte, error)
	GroupBaseList(status, descr bool,targ func(group []byte, status byte, descr []byte)) bool
}

type GroupRealtimeImpl struct {
	ArticleGroupEX
	List GroupListDB
}
func (g *GroupRealtimeImpl) GroupRealtimeQuery(group []byte) (number int64, low int64, high int64, ok bool) {
	number,low,high,ok = g.ArticleGroupEX.GroupRealtimeQuery(group)
	if ok { return }
	if grps,_ := g.List.GroupHeadFilterWithAuth(postauth.ARFeeder,[][]byte{group}); len(grps)==1 { number,low,high,ok = 0,0,0,true }
	return
}
func (g *GroupRealtimeImpl) GroupRealtimeList(targ func(group []byte, high, low int64, status byte)) bool {
	return g.List.GroupBaseList(true,false,func(group []byte, status byte, descr []byte){
		_, low , high , ok := g.GroupRealtimeQuery(group)
		if !ok { return }
		targ(group,high,low,status)
	})
}

var _ newspolyglot.GroupRealtimeDB = (*GroupRealtimeImpl)(nil)

type GroupStaticImpl struct {
	List GroupListDB
}

func (g *GroupStaticImpl) GroupStaticList(targ func(group []byte, descr []byte)) bool {
	return g.List.GroupBaseList(false,true,func(group []byte, status byte, descr []byte){
		targ(group,descr)
	})
}

var _ newspolyglot.GroupStaticDB = (*GroupStaticImpl)(nil)

type PostingImpl struct {
	Grp    ArticleGroupEX
	Dir    ArticleDirectEX
	
	Policy PostingPolicyLite
}
func (p *PostingImpl) ArticlePostingCheckPost() (possible bool) {
	return p.Policy!=nil
}
func (p *PostingImpl) ArticlePostingCheckPostId(id []byte) (wanted bool, possible bool) {
	possible = p.Policy!=nil
	wanted = !p.Dir.ArticleDirectStat(id)
	return
}

func (p *PostingImpl) ArticlePostingPost(headp *posting.HeadInfo, body []byte, ngs [][]byte, numbs []int64) (rejected bool, failed bool, err error) {
	ov := newspolyglot.AcquireArticleOverview()
	defer newspolyglot.ReleaseArticleOverview(ov)
	obj := newspolyglot.AcquireArticleObject()
	defer newspolyglot.ReleaseArticleObject(obj)
	
	ov.Subject = headp.Subject
	ov.From    = headp.From
	ov.Date    = headp.Date
	ov.MsgId   = headp.MessageId
	ov.Refs    = headp.References
	ov.Bytes   = int64(len(headp.RAW)+2+len(body))
	ov.Lines   = posting.CountLines(body)
	
	obj.Head = headp.RAW
	obj.Body = body
	
	decision := p.Policy.DecideLite(ngs,ov.Lines,ov.Bytes)
	
	exp := uint64(decision.ExpireAt.Unix())
	
	err = p.Dir.ArticleDirectStore(exp, ov,obj)
	if err!=nil { failed = true; return }
	
	err = p.Grp.StoreArticleInfos(ngs, numbs, exp, ov)
	if err!=nil {
		failed = true
		p.Dir.ArticleDirectRollback(ov.MsgId)
		return
	}
	
	return
}

var _ newspolyglot.ArticlePostingDB = (*PostingImpl)(nil)

/* ### */
