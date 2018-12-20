package pro

import (
	"sort"
	"testing"
)

var a = []float64{4, 2, 5, 7, 2, 1, 88, 1}

func BenchmarkSortFloat64 (b *testing.B){
	for i:=0; i< b.N; i++ {
		sort.Float64s(a)
	}
}


func BenchmarkSortFloat2Int1 (b *testing.B){
	for i:=0; i< b.N; i++ {
		SortFloat64FastV1(a)
	}
}

func BenchmarkSortFloat2Int2 (b *testing.B){
	for i:=0; i< b.N; i++ {
		SortFloat64FastV2(a)
	}
}
