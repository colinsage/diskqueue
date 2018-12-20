package diskqueue

import (
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"
	"unsafe"
)

func TestWrite(t *testing.T){
	os.RemoveAll("_data/")

	q := NewQueue("_data/", 10*1024*1024);
	q.Open()

	b := []byte("helloworld")
	ts := time.Now().UnixNano()

	msg := NewDefaultMessage(b, ts)
	q.Write(msg)

	q.Close()
}


func TestWriteCloseAndWrite(t *testing.T){
	os.RemoveAll("_data/")
	q := NewQueue("_data/", 10*1024*1024);
	q.Open()

	b := []byte("helloworld")
	ts := time.Now().UnixNano()
	msg := NewDefaultMessage(b, ts)
	q.Write(msg)

	q.Close()

	q.Open()

	b2 := []byte("hello2world2")
	ts2 := time.Now().UnixNano()

	msg2 := NewDefaultMessage(b2, ts2)
	q.Write(msg2)

	q.Close()
}

func TestString(t *testing.T){
	s := "hello, world"
	//hello := s[:5]
	//world := s[7:]

	s1 := "hello, world"[:5]
	s2 := "hello, world"[7:]

	fmt.Println("len(s):", (*reflect.StringHeader)(unsafe.Pointer(&s)).Len)   // 12
	fmt.Println("len(s1):", (*reflect.StringHeader)(unsafe.Pointer(&s1)).Len) // 5
	fmt.Println("len(s2):", (*reflect.StringHeader)(unsafe.Pointer(&s2)).Len) // 5

	sz := "中国abc"

	fmt.Println(len(sz))

	for i, c := range sz {
		fmt.Println(i, c)
	}
}