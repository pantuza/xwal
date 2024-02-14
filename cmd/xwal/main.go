package main

import (
	"fmt"

	"github.com/davecgh/go-spew/spew"
	"github.com/pantuza/xwal/internal/xwal"
	"github.com/pantuza/xwal/protobuf/xwalpb"
)

func main() {
	fmt.Println("xWAL library")
	entry := &xwalpb.WALEntry{
		LSN:  42,
		Data: []byte("fake data"),
		CRC:  42,
	}
	spew.Dump(entry)

	xwal, err := xwal.NewXWAL(*xwal.NewXWALConfig(""))
	if err != nil {
		fmt.Println(err.Error())
	}
	spew.Dump(xwal)
}
