package heap

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func TestGroupBy(t *testing.T){

	tv1 := make([]tv,0,100)
	tv2 := make([]tv,0,100)
	tv3 := make([]tv,0,100)

	rand.Seed(time.Now().UnixNano())
	s1 := rand.Int63n(100)
	s2 := rand.Int63n(100)
	s3 := rand.Int63n(100)
	for i:=0; i<100; i++{
		tv1 = append(tv1, tv{ts: s1 + int64(i*1), value: 1.1})
		tv2 = append(tv2, tv{ts: s2 + int64(i*2), value: 2.1})
		tv3 = append(tv3, tv{ts: s3 + int64(i*3), value: 3.1})
	}

	itr1 := NewIterator("m1", tv1)
	itr2 := NewIterator("m2", tv2)
	itr3 := NewIterator("m3", tv3)

	itrs := make([]*Iterator, 0, 3)
	itrs = append(itrs, itr2)
	itrs = append(itrs, itr1)
	itrs = append(itrs, itr3)


	m := NewMergeIterator(itrs)


	for {
		p:= m.Next()
		if p == nil {
			break
		}

		fmt.Printf("%v \n", p)
	}

}
