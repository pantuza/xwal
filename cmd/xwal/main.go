package main

import (
	"fmt"

	"github.com/davecgh/go-spew/spew"
	"github.com/pantuza/xwal/internal/xwal"
	"github.com/pantuza/xwal/protobuf/xwalpb"
)

func main() {
	fmt.Println("xWAL library")

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

		data := []byte(`{"name": "John", "age": 42, "city": "Belo Horizonte"}`) // any []byte fake data
		if err := xwal.Write(data); err != nil {
			panic(err)
		}
	}

	err = xwal.Replay(func(entries []*xwalpb.WALEntry) error {
		for _, entry := range entries {
			spew.Dump(entry)
		}
		return nil // Return nil or an appropriate error value
	}, 5)
	if err != nil {
		panic(err)
	}
	// spew.Dump(xwal)
}
