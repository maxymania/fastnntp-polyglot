/*
Copyright (c) 2018-2019 Simon Schmidt

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


package cassm

import "github.com/gocql/gocql"
import "time"
import "github.com/maxymania/fastnntp-polyglot"
import "github.com/maxymania/fastnntp-polyglot/gold"
import "github.com/byte-mug/golibs/msgpackx"

/*
A simple Datastore using one partition key per newsgroup.
It has the advantage, that it is very simple, and thus has low overhead,
and few moving parts.
But it has the disadvantage, that huge newsgroup may cause a skew distribution
across partitions.
*/
type SimpleGroupDB struct{
	Granularity
	unimplemented
	Session     *gocql.Session
	OnAssign    gocql.Consistency
	OnIncrement gocql.Consistency
}

/*
Validates the Object arguments such as OnIncrement.

Should be called before the object is being used, for reliability.
*/
func (g *SimpleGroupDB) Validate(){
	switch g.OnIncrement {
	case gocql.Any,gocql.Two,gocql.Three:
		g.OnIncrement = gocql.Quorum
	}
}

func (g *SimpleGroupDB) StoreArticleInfos(groups [][]byte, nums []int64, exp uint64, ov *newspolyglot.ArticleOverview) (err error) {
	var id []byte
	if id,err = msgpackx.Marshal(flattenV(ov)...); err!=nil { return err }
	
	gids := make([]gocql.UUID,len(groups))
	for i,group := range groups {
		gids[i],err = getUUID(g.Session,group)
		if err!=nil { return }
	}
	ept,coarse := g.convert(exp)
	secs := int64(time.Until(time.Unix(int64(ept),0))/time.Second) + 1
	
	batch := g.Session.NewBatch(gocql.UnloggedBatch)
	batch.SetConsistency(g.OnAssign)
	ctrbt := g.Session.NewBatch(gocql.CounterBatch)
	ctrbt.SetConsistency(g.OnIncrement)
	for i,gid := range gids {
		batch.Query(`
			INSERT INTO agstat (identifier,articlenum,overview) VALUES (?,?,?) USING TTL ?
		`,gid,nums[i],id,secs)
		ctrbt.Query(`
			UPDATE agrpcnt SET number = number + 1 WHERE identifier = ? AND livesuntil = ?
		`,gid,coarse)
	}
	err = g.Session.ExecuteBatch(batch)
	if err!=nil { return }
	err = g.Session.ExecuteBatch(ctrbt)
	
	return
}

func (g *SimpleGroupDB) GroupRealtimeQuery(group []byte) (number int64, low int64, high int64, ok bool) {
	u,err := peekUUID(g.Session,group)
	if err!=nil { return }
	now := time.Now().UTC().Unix()
	
	ok = g.Session.Query(`
		SELECT MIN(articlenum),MAX(articlenum) FROM agstat WHERE identifier = ?
	`,u).Scan(&low,&high)!=nil
	ok2 := g.Session.Query(`
		SELECT SUM(number) FROM agrpcnt WHERE identifier = ? AND livesuntil >= ?
	`,u,now).Scan(&number)
	if ok2!=nil {
		number = 1+high-low
	} else {
		num := 1+high-low
		if number > num { number = num }
	}
	return
}

// Efficient traversal of a newsgroup.
func (g *SimpleGroupDB) ArticleGroupList(group []byte, first, last int64, targ func(int64)) {
	u,err := peekUUID(g.Session,group)
	if err!=nil { return }
	iter := g.Session.Query(`
		SELECT articlenum
		FROM agstat
		WHERE identifier = ? AND articlenum >= ? AND articlenum <= ?
	`,u,first,last).PageSize(1<<16).Prefetch(.25).Iter()
	defer iter.Close()
	var num int64
	for iter.Scan(&num) {
		targ(num)
	}
}

func (g *SimpleGroupDB) ArticleGroupOverview(group []byte, first, last int64, targ func(int64, *newspolyglot.ArticleOverview)) {
	u,err := peekUUID(g.Session,group)
	if err!=nil { return }
	iter := g.Session.Query(`
		SELECT articlenum, overview
		FROM agstat
		WHERE identifier = ? AND articlenum >= ? AND articlenum <= ?
	`,u,first,last).PageSize(1<<16).Prefetch(.25).Iter()
	defer iter.Close()
	var num int64
	var id []byte
	ov := new(newspolyglot.ArticleOverview)
	ii := flattenP(ov)
	for iter.Scan(&num,&id) {
		if msgpackx.Unmarshal(id,ii...)!=nil { continue }
		targ(num,ov)
	}
}

func (g *SimpleGroupDB) ArticleGroupStat(group []byte, num int64, id_buf []byte) ([]byte, bool) {
	u,err := peekUUID(g.Session,group)
	if err!=nil { return nil,false }
	iter := g.Session.Query(`
		SELECT overview FROM agstat WHERE identifier = ? AND articlenum = ?
	`,u,num).Iter()
	defer iter.Close()
	var id []byte
	ok := iter.Scan(&id)
	if ok { id,ok = getMsgId(id,id_buf) }
	return id,ok
}
func (g *SimpleGroupDB) ArticleGroupMove(group []byte, i int64, backward bool, id_buf []byte) (ni int64, id []byte, ok bool) {
	u,err := peekUUID(g.Session,group)
	if err!=nil { return }
	sym := ">"
	dir := "ASC"
	if backward { sym = "<"; dir = "DESC" }
	iter := g.Session.Query(`
		SELECT articlenum,overview FROM agstat WHERE identifier = ? AND articlenum `+sym+` ? ORDER BY articlenum `+dir+`
	`,u,i).Iter()
	defer iter.Close()
	ok = iter.Scan(&ni,&id)
	
	if ok { id,ok = getMsgId(id,id_buf); return }
	
	return
}

var _ gold.ArticleGroupEX = (*SimpleGroupDB)(nil)

