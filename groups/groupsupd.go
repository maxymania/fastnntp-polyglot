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


package groups

import "sync"
import "github.com/vmihailenco/msgpack"
import "fmt"

type TablePair struct {
	GroupID []byte
	Value   []byte
}

type BackendTable interface {
	// ids must be byte arrays.
	GetPairs([]interface{}) ([]TablePair,error)
	SetPairs([]TablePair) error
}


type GroupHeadActor struct{
	sync.Mutex
	tabbuf  [256]TablePair
	keybuf  [256]interface{}
	cache   map[string]*GroupEntry
	backend BackendTable
}
func NewGroupHeadActor(bt BackendTable) *GroupHeadActor {
	gha := new(GroupHeadActor)
	gha.cache   = make(map[string]*GroupEntry)
	gha.backend = bt
	return gha
}
func (g *GroupHeadActor) pull(groups [][]byte) error {
	buf := g.keybuf[:0]
	for _,group := range groups {
		_,ok := g.cache[string(group)]
		if ok { continue }
		buf = append(buf,group)
	}
	tab,err := g.backend.GetPairs(buf)
	if err!=nil { return err }
	for _,pair := range tab {
		ge := new(GroupEntry)
		msgpack.Unmarshal(pair.Value,&ge)
		g.cache[string(pair.GroupID)] = ge
	}
	for _,group := range groups {
		_,ok := g.cache[string(group)]
		if !ok { return fmt.Errorf("No such group %q",group) }
	}
	return nil
}
func (g *GroupHeadActor) MoveDown(group []byte) (int64,error) {
	g.Lock(); defer g.Unlock()
	err := g.pull([][]byte{group})
	if err!=nil { return 0,err }
	tab := g.tabbuf[:0]
	grp := g.cache[string(group)]
	bak := *grp
	grp.MoveDown()
	data,_ := msgpack.Marshal(grp)
	tab = append(tab,TablePair{group,data})
	err = g.backend.SetPairs(tab)
	if err!=nil {
		*grp = bak
		return 0,err
	}
	return grp.High1,nil
}

func (g *GroupHeadActor) GroupHeadInsert(groups [][]byte, buf []int64) ([]int64, error) {
	g.Lock(); defer g.Unlock()
	err := g.pull(groups)
	if err!=nil { return nil,err }
	if cap(buf)<len(groups) { buf = make([]int64,len(groups)) } else { buf = buf[:len(groups)] }
	
	tab := g.tabbuf[:0]
	for i,group := range groups {
		ge := g.cache[string(group)]
		buf[i] = ge.Increment()
		data,_ := msgpack.Marshal(ge)
		tab = append(tab,TablePair{group,data})
	}
	err = g.backend.SetPairs(tab)
	if err!=nil {
		// If we cant publish the result, perform a Rollback.
		for i,group := range groups {
			g.cache[string(group)].Rollback(buf[i])
		}
		return nil,err
	}
	return buf,nil
}
func (g *GroupHeadActor) GroupHeadRevert(groups [][]byte, nums []int64) error {
	g.Lock(); defer g.Unlock()
	err := g.pull(groups)
	if err!=nil { return err }
	tab := g.tabbuf[:0]
	for i,group := range groups {
		ge := g.cache[string(group)]
		ge.Rollback(nums[i])
		data,_ := msgpack.Marshal(ge)
		tab = append(tab,TablePair{group,data})
	}
	return g.backend.SetPairs(tab)
}
