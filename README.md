[![XWAL](https://github.com/pantuza/xwal/actions/workflows/main.yml/badge.svg)](https://github.com/pantuza/xwal/actions/workflows/main.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/pantuza/xwal)](https://pkg.go.dev/github.com/pantuza/xwal)
[![Go Report Card](https://goreportcard.com/badge/github.com/pantuza/xwal)](https://goreportcard.com/report/github.com/pantuza/xwal)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![Library Latest Version](https://img.shields.io/badge/Package_Latest_Version-blue)](https://github.com/pantuza/xwal/releases)


# xwal
A Cross, thread-safe and buffered Write Ahead Log (WAL) library for Golang applications.

* Cross? Yes, we mean you can choose your WAL Backend: Local Filesystem, AWS s3, etc.
* Thread-safe? Yes, once you have a xwal instance, you can safelly call its methods concurrently.
* Buffered? Yes, xwal uses an In Memory Buffer that flushes to the chosen WAL Backend asynchronosly.

## Usage

```go
cfg := xwal.NewXWALConfig("") // Uses default configuration

xwal, err := xwal.NewXWAL(cfg) // Creates a new WAL instance
if err != nil {
  panic(err)
}
defer xwal.Close()

// Your WAL is ready to be used. Thus, elsewhere in your code you can call:
err := xwal.Write([]byte(`{"data": "any data in any format serialized to bytes you want to persist in the WAL"}`))
if err != nil {
  panic(err)
}

// Let's suppose your remote backend is failing and you need to Replay data from WAL to it.
// You simply provide xwal a callback function that speaks to your backend and xwal will
// make sure to give you the stored data in the order you want:
func myCallback(entries []*xwalpb.WALEntry) error {
  for _, entry := range entries {
    // Here you can send your data to your backend or do any computation you want
  }
  return nil
}

err = xwal.Replay(myCallback, 5, false) // Replay reads entries from the WAL and sends to your callback function
if err != nil {
  panic(err)
}
```

## Available Backends

| Backend | Description   | Examples   |
|-------------- | -------------- | -------------- |
| **Local FS**    | WAL entries are stored on the local filesystem     | [localfs](./examples/)     |
| **AWS s3**    | WAL entries are stored remotely on AWS s3 service    | [s3](./examples/)  |


## Features
TODO: Describe all features Here

## Installation
```bash
go get github.com/pantuza/xwal
```

## Contributing
TODO: Describe how to contribute

## License
* [MIT License](./LICENSE)

## Knowledge Base
* [Write Ahead Log](https://en.wikipedia.org/wiki/Write-ahead_logging)
* [Who needs a Write Ahead Log?](https://www.cockroachlabs.com/blog/who-needs-a-write-ahead-log/)
* [xWAL examples](./examples)
