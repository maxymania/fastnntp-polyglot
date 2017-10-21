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
import "time"

func joinUp(buf []byte,s []byte,i int64) []byte {
	u := uint64(i)
	buf = append(buf[:0],s...)
	buf = append(buf,
	byte(u>>40),
	byte(u>>32),
	byte(u>>24),
	byte(u>>16),
	byte(u>>8),
	byte(u))
	return buf
}

type GroupNumValue struct{
	Partit   []byte
	Clustr   int
	OrigNo   int64
	Value    []byte
	ExpireAt time.Time
}
func GnvTable(ksp gocassa.KeySpace) gocassa.Table {
	return ksp.Table("v2grpov",&GroupNumValue{},gocassa.Keys{
		PartitionKeys: []string{"Partit"},
		ClusteringColumns: []string{"Clustr"},
	}).WithOptions(gocassa.Options{
		TableName: "v2grpov", // Yes, We override the Table name.
	})
}

func mkGNV(group []byte,num int64,value []byte,expa time.Time) GroupNumValue {
	return GroupNumValue{joinUp(make([]byte,0,len(group)+6),group,num>>16),int(num&0xffff),num,value,expa}
}

func getGNV(tab gocassa.Table,group []byte,num int64) gocassa.Filter {
	return tab.Where(gocassa.Eq("Partit",joinUp(nil,group,num>>16)),gocassa.Eq("Clustr",int(num&0xffff)))
}

var getNextGNV_Options = gocassa.Options{
	Limit: 1,
}
func getNextGNV(tab gocassa.Table,group []byte,num int64) (int64,[]byte,bool) {
	pn := num>>16
	part := joinUp(nil,group,pn)
	gnvs := []GroupNumValue{}
	tab.Where(gocassa.Eq("Partit",part),gocassa.GT("Clustr",int(num&0xffff))).Read(&gnvs).WithOptions(getNextGNV_Options).Run()
	for i := 0; i<128 ; i++ {
		if len(gnvs)>0 {
			return gnvs[0].OrigNo,gnvs[0].Value,true
		}
		pn++
		part = joinUp(part,group,pn)
		tab.Where(gocassa.Eq("Partit",part)).Read(&gnvs).WithOptions(getNextGNV_Options).Run()
	}
	return 0,nil,false
}

var getPrevGNV_Options = gocassa.Options{
	ClusteringOrder:[]gocassa.ClusteringOrderColumn{{gocassa.DESC,"Clustr"}},
	Limit:1,
}
func getPrevGNV(tab gocassa.Table,group []byte,num int64) (int64,[]byte,bool) {
	pn := num>>16
	part := joinUp(nil,group,pn)
	gnvs := []GroupNumValue{}
	tab.Where(gocassa.Eq("Partit",part),gocassa.LT("Clustr",int(num&0xffff))).Read(&gnvs).WithOptions(getPrevGNV_Options).Run()
	for i := 0; i<128 ; i++ {
		if len(gnvs)>0 {
			return gnvs[0].OrigNo,gnvs[0].Value,true
		}
		if pn==0 { break } // if we are in partition 0, then stop.
		pn--
		part = joinUp(part,group,pn)
		tab.Where(gocassa.Eq("Partit",part)).Read(&gnvs).WithOptions(getPrevGNV_Options).Run()
	}
	return 0,nil,false
}

func getLoopGNV(tab gocassa.Table,group []byte,first, last int64,fu func(int64,[]byte) bool) {
	pn := first>>16
	lpn := last>>16
	part := joinUp(nil,group,pn)
	gnvs := []GroupNumValue{}
	if pn==lpn {
		tab.Where(gocassa.Eq("Partit",part),gocassa.GTE("Clustr",int(first&0xffff)),gocassa.LTE("Clustr",int(last&0xffff))).Read(&gnvs).Run()
	}else{
		tab.Where(gocassa.Eq("Partit",part),gocassa.GTE("Clustr",int(first&0xffff))).Read(&gnvs).Run()
	}
	nf := 0
	for {
		if len(gnvs)==0 {
			nf++
			if nf>256 { break }
		} else {
			nf = 0
		}
		for _,gnv := range gnvs {
			if gnv.OrigNo>last { return }
			if !fu(gnv.OrigNo,gnv.Value) { return }
		}
		pn++
		if pn>lpn { break } // lpn is the last partition having
		part = joinUp(part,group,pn)
		if pn==lpn {
			tab.Where(gocassa.Eq("Partit",part),gocassa.LTE("Clustr",int(last&0xffff))).Read(&gnvs).Run()
		} else {
			tab.Where(gocassa.Eq("Partit",part)).Read(&gnvs).Run()
		}
	}
}




