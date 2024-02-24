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

	cfg := xwal.NewXWALConfig("")
	cfg.BufferSize = 1
	cfg.BufferEntriesLength = 5

	xwal, err := xwal.NewXWAL(cfg)
	if err != nil {
		panic(err)
	}
	defer xwal.Close()

	for i := 0; i < 12; i++ {
		fmt.Printf("Writing entry %d\n", i)
		if err := xwal.Write(entry); err != nil {
			panic(err)
		}
	}

	entries, err := xwal.Replay()
	if err != nil {
		panic(err)
	}

	for _, entry := range entries {
		spew.Dump(entry)
	}

	// spew.Dump(xwal)
}
