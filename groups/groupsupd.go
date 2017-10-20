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
import "time"

func jenkins(group []byte) uint32 {
	hash := uint32(0)
	for _,k := range group {
		hash += uint32(k)
		hash += hash << 10
		hash ^= hash >> 6
	}
	hash += hash << 3;
	hash ^= hash >> 11;
	hash += hash << 15;
	return hash
}

func joinerr(e1, e2 error) error {
	if e1==nil { return e2 }
	return e1
}

type timedGroupRTE struct{
	GroupRTE
	valid time.Time
}

// The count is assumed to be dense
type GroupRTE struct{
	Start,High,Lost int64
}

type GroupUpdater interface{
	GroupLoad(group []byte,rte *GroupRTE) error
	GroupStore(group []byte,rte *GroupRTE) error
}

type updater struct{
	sync.Mutex
	cache map[string]*timedGroupRTE
}
func (u *updater) get(group []byte, gu GroupUpdater) (*timedGroupRTE,error) {
	now := time.Now()
	var grt *timedGroupRTE
	var ok  bool
	if u.cache!=nil {
		grt,ok = u.cache[string(group)]
		if !ok {
		} else if now.After(grt.valid) {
			delete(u.cache,string(group))
		} else  {
			return grt,nil
		}
	}
	grt = new(timedGroupRTE)
	err := gu.GroupLoad(group,&(grt.GroupRTE))
	if err!=nil { return nil,err }
	grt.valid = now.Add(time.Hour)
	if u.cache==nil {
		u.cache =  map[string]*timedGroupRTE{ string(group): grt }
	}else{
		u.cache[string(group)] = grt
	}
	return grt,nil
}
func (u *updater) increment(group []byte, gu GroupUpdater) (int64,error) {
	u.Lock(); defer u.Unlock()
	grt,err := u.get(group,gu)
	if err!=nil { return 0,err }
	grt.High++
	return grt.High,gu.GroupStore(group,&(grt.GroupRTE))
}
func (u *updater) revert(group []byte,i int64, gu GroupUpdater) (error) {
	u.Lock(); defer u.Unlock()
	grt,err := u.get(group,gu)
	if err!=nil { return err }
	if grt.High==i { grt.High-- } else { grt.Lost++ }
	return gu.GroupStore(group,&(grt.GroupRTE))
}
type GroupHead struct {
	GU GroupUpdater
	u [256]updater
}
func (g *GroupHead) GroupHeadInsert(groups [][]byte, buf []int64) ([]int64, error) {
	if cap(buf)<len(groups) { buf = make([]int64,len(groups)) }
	buf = buf[:0]
	for _,group := range groups {
		i,e := g.u[jenkins(group)&255].increment(group,g.GU)
		if e!=nil {
			g.GroupHeadRevert(groups[:len(buf)],buf)
			return nil,e
		}
		buf = append(buf,i)
	}
	return buf,nil
}
func (g *GroupHead) GroupHeadRevert(groups [][]byte, nums []int64) (e error) {
	for i,group := range groups {
		e = joinerr(e,g.u[jenkins(group)&255].revert(group,nums[i],g.GU))
	}
	return
}

