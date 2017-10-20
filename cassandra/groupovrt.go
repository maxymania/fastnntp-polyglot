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
import "bytes"
import "fmt"

type GroupRealtime struct{
	Group []byte
	Number,Low,High int64
	Status int
}

type GroupStatic struct{
	Group []byte
	Descr []byte
}

/*
This class implements GroupHeadCache, GroupHeadDB, GroupRealtimeDB and GroupStaticDB
Most Methods are eighter Brittle or Inefficient.

Warning: it is not recommended to use it as GroupHeadDB, unless it is used from a single node
or with serious synchronization mechanisms. Otherwise, a race-condition will occure.
*/
type GroupManager struct{
	ks gocassa.KeySpace
	rt gocassa.Table
	st gocassa.Table
}
func NewGroupManager(ks gocassa.KeySpace) *GroupManager {
	rt := ks.Table("grprealtime",&GroupRealtime{},gocassa.Keys{
		PartitionKeys: []string{"Group"},
	})
	st := ks.Table("grpstatic",&GroupStatic{},gocassa.Keys{
		PartitionKeys: []string{"Group"},
	}).WithOptions(gocassa.Options{
		CompactStorage: true,
	})
	return &GroupManager{
		ks,
		rt,
		st,
	}
}
func (g *GroupManager) Initialize() {
	g.rt.CreateIfNotExist()
	g.st.CreateIfNotExist()
}
func (g *GroupManager) GroupStaticList(targ func(group []byte, descr []byte)) bool {
	gs := []GroupStatic{}
	if g.st.Where().Read(&gs).Run()!=nil { return false }
	for _,gse := range gs { targ(gse.Group,gse.Descr) }
	return true
}
func (g *GroupManager) GroupRealtimeQuery(group []byte) (number int64, low int64, high int64, ok bool) {
	gs := GroupRealtime{}
	err := g.rt.Where(gocassa.Eq("Group",group)).ReadOne(&gs).Run()
	fmt.Println(err,gs)
	if err!=nil { return }
	return gs.Number,gs.Low,gs.High,true
}
func (g *GroupManager) GroupRealtimeList(targ func(group []byte, high, low int64, status byte)) bool {
	gs := []GroupRealtime{}
	if g.rt.Where().Read(&gs).Run()!=nil { return false }
	for _,gse := range gs { targ(gse.Group,gse.High,gse.Low,byte(gse.Status)) }
	return true
}
func (g *GroupManager) GroupHeadFilter(groups [][]byte) ([][]byte, error) {
	keys := make([]interface{},len(groups))
	for i,g := range groups { keys[i]=g }
	gs := []GroupStatic{}
	err := g.st.Where(gocassa.In("Group",keys...)).Read(&gs).Run()
	if err!=nil { return nil,err }
	ng := groups[:0]
	for _,gse := range gs {
		ng = append(ng,gse.Group)
	}
	return ng,nil
}

func (g *GroupManager) AdmAddGroup(group, descr []byte) error {
	gra := []GroupRealtime{}
	gsa := []GroupStatic{}
	op := g.st.Where(gocassa.Eq("Group",group)).Read(&gsa)
	op = op.Add(g.rt.Where(gocassa.Eq("Group",group)).Read(&gra))
	err := op.Run()
	if err!=nil { return err }
	if len(gra)>0 || len(gsa)>0 {
		return fmt.Errorf("Group exists")
	}
	op = g.st.Set(GroupStatic{Group:group,Descr:descr})
	err = op.Add(g.rt.Set(GroupRealtime{Group:group})).Run()
	return err
}
func (g *GroupManager) AdmGroupChangeState(group []byte, state byte) error {
	gs := GroupRealtime{}
	err := g.rt.Where(gocassa.Eq("Group",group)).ReadOne(&gs).Run()
	if err!=nil { return err }
	gs.Status = int(state)
	return g.rt.Set(gs).Run()
}

/*
Warning: it is not recommended to use this Method, unless it is used from a single node
or with serious synchronization mechanisms. Otherwise, a race-condition will occure.
*/
func (g *GroupManager) GroupHeadInsert(groups [][]byte, buf []int64) ([]int64, error) {
	keys := make([]interface{},len(groups))
	for i,g := range groups { keys[i]=g }
	gs := []GroupRealtime{}
	err := g.rt.Where(gocassa.In("Group",keys...)).Read(&gs).Run()
	fmt.Println("Select where group in ...: ",err)
	if err!=nil { return nil,err }
	if len(gs)!=len(groups) {
		return nil,fmt.Errorf("Groups not found")
	}
	if cap(buf)<len(groups) {
		buf = make([]int64,len(groups))
	} else {
		buf = buf[:len(groups)]
	}
	for i,group := range groups {
		for e,gse := range gs {
			if bytes.Equal(gse.Group,group) {
				gse.High++
				gse.Number++
				if gse.Low==0 { gse.Low++ }
				buf[i] = gse.High
				gs[e] = gse
				break
			}
		}
	}
	var op gocassa.Op
	for i,gse := range gs {
		nop := g.rt.Set(gse)
		if i==0 {
			op = nop
		} else {
			op = op.Add(nop)
		}
	}
	err = op.Run()
	fmt.Println(gs)
	fmt.Println("Update: ",err)
	if err!=nil { return nil,err }
	return buf,nil
}

/*
Warning: it is not recommended to use this Method, unless it is used from a single node
or with serious synchronization mechanisms. Otherwise, a race-condition will occure.
*/
func (g *GroupManager) GroupHeadRevert(groups [][]byte, nums []int64) error {
	keys := make([]interface{},len(groups))
	for i,g := range groups { keys[i]=g }
	gs := []GroupRealtime{}
	err := g.rt.Where(gocassa.In("Group",keys...)).Read(&gs).Run()
	fmt.Println("Select where group in ...: ",err)
	if err!=nil { return err }
	if len(gs)!=len(groups) {
		return fmt.Errorf("Groups not found")
	}
	for i,group := range groups {
		for e,gse := range gs {
			if bytes.Equal(gse.Group,group) {
				if nums[i]==gse.High {
					gse.High--
				} else if nums[i]==gse.Low {
					gse.Low++
				}
				gse.Number--
				gs[e] = gse
				break
			}
		}
	}
	var op gocassa.Op
	for i,gse := range gs {
		nop := g.rt.Set(gse)
		if i==0 {
			op = nop
		} else {
			op = op.Add(nop)
		}
	}
	err = op.Run()
	fmt.Println("Update: ",err)
	if err!=nil { return err }
	return nil
}

