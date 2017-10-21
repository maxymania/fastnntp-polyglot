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

import "sort"
import "github.com/vmihailenco/msgpack"
import "fmt"

func growTablePairArray(i []TablePair,wanted int) []TablePair {
	if cap(i)<wanted {
		n := make([]TablePair,wanted)
		copy(n,i)
		return n
	}
	return i[:wanted]
}

type ImportTable interface{
	// Reads the table.
	GetTable() ([]TablePair,error)
}

type GroupOverviewStatic struct{
	Map    map[string]TablePair
	Array  []string
	IArray map[string]int // Inverse array
}

func ImportGroupOverviewStatic(i ImportTable) (*GroupOverviewStatic,error) {
	tab,err := i.GetTable()
	if err!=nil { return nil,err }
	mp := make(map[string]TablePair,len(tab))
	ar := make([]string,len(tab))
	ia := make(map[string]int,len(tab))
	for i,pair := range tab {
		s := string(pair.GroupID)
		mp[s] = pair
		ar[i] = s
	}
	sort.Strings(ar)
	for i,s := range ar { ia[s] = i }
	return &GroupOverviewStatic{mp,ar,ia},nil
}


type GroupOverview struct{
	Overview *GroupOverviewStatic
	// This should be the same table, that is also used by GroupHeadActor.
	Realtime BackendTable
}

func (g *GroupOverview) GroupStaticList(targ func(group []byte, descr []byte)) bool {
	if g.Overview==nil { return false }
	mp := g.Overview.Map
	ar := g.Overview.Array
	for _,s := range ar {
		pair := mp[s]
		targ(pair.GroupID,pair.Value[1:])
	}
	return true
}
func (g *GroupOverview) GroupRealtimeQuery(group []byte) (number int64, low int64, high int64, ok bool) {
	if g.Overview==nil { return }
	if _,tok := g.Overview.Map[string(group)] ; !tok { return }
	tab,e := g.Realtime.GetPairs([]interface{}{group})
	if e!=nil || len(tab)==0 { return }
	ge := new(GroupEntry)
	if msgpack.Unmarshal(tab[0].Value,&ge)!=nil { return }
	low,high,number = ge.HlStats()
	ok = true
	return
}
func (g *GroupOverview) GroupRealtimeList(targ func(group []byte, high, low int64, status byte)) bool {
	if g.Overview==nil { return false }
	mp := g.Overview.Map
	ar := g.Overview.Array
	ia := g.Overview.IArray
	lar := len(ar)
	if lar==0 { return false }
	
	// TODO: Allocations are EVIL
	ids := make([]interface{},lar)
	for i,s := range ar {
		pair := mp[s]
		ids[i] = pair.GroupID
	}
	tab,e := g.Realtime.GetPairs(ids)
	if e!=nil || len(tab)==0 { return false }
	
	tab = growTablePairArray(tab,lar)
	
	// Sort the table into its original order using the inverse Array.
	{
		cc := 0
		i := 0
		for {
			gid := tab[i].GroupID
			if len(gid)!=0 {
				j,ok := ia[string(gid)]
				if ok && i!=j {
					tab[i],tab[j] = tab[j],tab[i]
					cc++
					if cc>lar { break } // Too many Iterations: break!
					continue
				}
			}
			i++
			if i>=lar { break }
		}
	}
	
	ge := new(GroupEntry)
	for _,pair := range tab {
		if len(pair.GroupID)==0 { continue } // Skip empty entries.
		sp := mp[string(pair.GroupID)]
		if len(sp.Value)==0 { continue } // Safety first.
		if msgpack.Unmarshal(pair.Value,&ge)!=nil { continue }
		low,high,_ := ge.HlStats()
		targ(pair.GroupID,high,low,sp.Value[0])
	}
	
	return true
}
func (g *GroupOverview) GroupHeadFilter(groups [][]byte) ([][]byte, error) {
	if g.Overview==nil { return nil,fmt.Errorf("Pathological case: g.Overview is not initialized") } // No groups!
	mp := g.Overview.Map
	i := 0
	for _,group := range groups {
		grp := mp[string(group)]
		if len(grp.Value)==0 { continue }
		if grp.Value[0]!='y' { continue }
		groups[i] = group
		i++
	}
	return groups[:i],nil
}


