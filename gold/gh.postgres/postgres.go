/*
MIT License

Copyright (c) 2020 Simon Schmidt

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


package postgres

import "database/sql"
import "github.com/lib/pq"
import "github.com/maxymania/fastnntp-polyglot/gold/gh.generic"

type PsqlBulkAllocator struct {
	DB *sql.DB
}
func (p *PsqlBulkAllocator) Init() {
	p.DB.Exec(`
		CREATE TABLE groupheads (
			ghnam bytea primary key,
			ghctr bigint default 0,
			ghlst bigint[] default '{}'::bigint[],
			ghtmp bigint[]
		)
	`)
}

func (p *PsqlBulkAllocator) AllocIds(group []byte, buf []uint64) ([]uint64, error) {
	var err error
	
	{
		var nctr int64
		
		err = p.DB.QueryRow(`
			update groupheads set ghctr = ghctr+$2
				where ghnam = $1
				and coalesce(array_length(ghlst,1),0)=0
			returning ghctr;
		`,group,len(buf)).Scan(&nctr)
		if err==nil {
			n := len(buf)-1
			for i := range buf {
				buf[n-i] = uint64(nctr)
				nctr--
			}
			return buf,nil
		}
		if err!=sql.ErrNoRows { return nil,err }
	}
	
	{
		var array pq.Int64Array
		
		err = p.DB.QueryRow(`
			update groupheads set ghtmp = ghlst[:$2], ghlst = ghlst[1+$2:] where ghnam = $1
			returning ghtmp
		`,group,len(buf)).Scan(&array)
		
		if err==sql.ErrNoRows { goto noRows }
		if err!=nil { return nil,err }
		
		if len(array)>len(buf) { array = array[:len(buf)] }
		
		for i,num := range array { buf[i] = uint64(num) }
		return buf[:len(array)],nil
	}
	
	noRows:
	_,err = p.DB.Exec(`insert into groupheads (ghnam,ghctr) values ($1,$2)`,group,len(buf))
	if err!=nil { return nil,err }
	
	for i := range buf { buf[i] = uint64(i+1) }
	return buf,nil
}
func (p *PsqlBulkAllocator) RevertIds(group []byte, buf []uint64) error {
	array := make(pq.Int64Array,len(buf))
	for i,num := range buf { array[i] = int64(num) }
	//_,err := p.DB.Exec(`update groupheads set ghlst = ghlst || ($2)::bigint[] where ghnam = $1`,group,array)
	_,err := p.DB.Exec(`
		update groupheads set ghlst =
			array(select distinct unnest(ghlst || ($2)::bigint[]) order by 1)
		where ghnam = $1
	`,group,array)
	return err
}

var _ generic.BulkAllocator = (*PsqlBulkAllocator)(nil)

// #
