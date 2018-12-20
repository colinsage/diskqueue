package heap

import "container/heap"

type tv struct {
	ts int64
	value float64
}

type point struct {
	name string

	tv  tv
}


func (p *point) Clone() *point{

	return &point{
		name : p.name,
		tv : p.tv,
	}
}
type Iterator struct {
	key string
	tv  []tv

	buf  *point

	index int
	length int
}

func NewIterator(key string, tv []tv) *Iterator{
	return &Iterator{
		key: key,
		tv : tv,
		length: len(tv),
		index : 0,
	}
}

func (it *Iterator) next() (p *point) {
	if it.buf != nil {
		p = it.buf
		it.buf = nil
		return
	}

	if it.index > it.length -1{
		return
	}

	cur := it.tv[it.index]
	p = &point{
		name: it.key,
		tv : cur,
	}
	it.index++
	return
}

func (it *Iterator) push(p *point){
	it.buf = p
}

type IteratorHeap struct {
	items  []*IteratorHeapItem
}

func (ith *IteratorHeap) Len() int{
	return len(ith.items)
}

func (ith *IteratorHeap) Swap(i,j int) {
	ith.items[i], ith.items[j] = ith.items[j], ith.items[i]
}

func (ith *IteratorHeap) Less(i, j int) bool{
	x, y := ith.items[i].point, ith.items[j].point

	if x.tv.ts != y.tv.ts {
		return x.tv.ts < y.tv.ts
	}

	if x.name != y.name {
		return x.name < y.name
	}



	return false
}

func (ith *IteratorHeap) Push(x interface{}) {
	ith.items = append(ith.items, x.(*IteratorHeapItem))
}

func (ith *IteratorHeap) Pop() interface{} {
	old := ith.items
	n := len(old)
	item := old[n-1]
	ith.items = old[0 : n-1]
	return item
}
type IteratorHeapItem struct {
	point *point
	it  *Iterator
}

type MergeIterator struct {
	its  []*Iterator

	heap  *IteratorHeap

	init  bool
}

func NewMergeIterator (its []*Iterator) *MergeIterator{

	itr := &MergeIterator{
		its : its,
	}
	itr.heap = &IteratorHeap{
		items : make([]*IteratorHeapItem, 0, len(its)),
	}
	for _, it := range its {
		itr.heap.items = append(itr.heap.items, &IteratorHeapItem{it :it})
	}

	return itr
}

func (mit * MergeIterator) Next() *point{
	if !mit.init{
		for _, item := range mit.heap.items{
			item.point = item.it.next()
		}

		heap.Init(mit.heap)
		mit.init = true
	}

	if len(mit.heap.items) == 0 {
		return nil
	}

	item := heap.Pop(mit.heap).(*IteratorHeapItem)
	if item.point == nil {
		return nil
	}

	// Copy the point for return.
	p := item.point.Clone()

	if item.point = item.it.next(); item.point != nil {
		heap.Push(mit.heap, item)
	}

	return p
}