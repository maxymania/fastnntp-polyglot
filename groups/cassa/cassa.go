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


package cassa

import "github.com/gocassa/gocassa"
import "github.com/maxymania/fastnntp-polyglot/groups"

type CassaTable struct{
	mpt gocassa.MapTable
}
func NewCassaTable(ks gocassa.KeySpace, n string) *CassaTable {
	mpt := ks.MapTable(n,"GroupID",&groups.TablePair{}).
		WithOptions(gocassa.Options{CompactStorage:true})
	return &CassaTable{
		mpt,
	}
}
func (c *CassaTable) Initialize() {
	c.mpt.CreateIfNotExist()
}
func (c *CassaTable) GetPairs(ids []interface{}) (tp []groups.TablePair,e error) {
	e = c.mpt.MultiRead(ids,&tp).Run()
	return
}
func (c *CassaTable) SetPairs(tab []groups.TablePair) error {
	var op gocassa.Op
	for i,pair := range tab {
		nop := c.mpt.Set(pair)
		if i==0 { op = nop } else { op = op.Add(nop) }
	}
	if op==nil { return nil }
	return op.Run()
}

type CassaStaticTable struct{
	tab gocassa.Table
}
func NewCassaStaticTable(ks gocassa.KeySpace, n string) *CassaStaticTable {
	return &CassaStaticTable{
		ks.Table(n,&groups.TablePair{},gocassa.Keys{
			PartitionKeys: []string{"GroupID"},
		}).WithOptions(gocassa.Options{CompactStorage:true}),
	}
}
func (c *CassaStaticTable) Initialize() {
	c.tab.CreateIfNotExist()
}
func (c *CassaStaticTable) GetTable() (ts []groups.TablePair,e error) {
	e = c.tab.Where().Read(&ts).Run()
	return
}
func (c *CassaStaticTable) SetRecord(pair groups.TablePair) error {
	return c.tab.Set(pair).Run()
}


