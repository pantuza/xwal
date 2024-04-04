package main

import (
	"encoding/base64"
	"fmt"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/pantuza/xwal/internal/xwal"
	"github.com/pantuza/xwal/protobuf/xwalpb"
)

const (
	NumberOfClients = 10

	ReplayFrequency = 30 * time.Second
)

// The wal variable is a global variable that represents the write-ahead log
var wal *xwal.XWAL

func handlerWithDelay(w http.ResponseWriter, r *http.Request) {
	delay := time.Duration(rand.Intn(1000)) * time.Millisecond

	time.Sleep(delay) // Simulate work by sleeping
	fmt.Fprintf(w, "Finished work with %v delay\n", delay)
}

// startServer initializes and starts the HTTP server
func startServer() {
	http.HandleFunc("/route1", handlerWithDelay)
	http.HandleFunc("/route2", handlerWithDelay)
	http.HandleFunc("/route3", handlerWithDelay)

	go func() {
		if err := http.ListenAndServe(":8080", nil); err != nil {
			panic(err)
		}
	}()
}

// startClient starts an infinite loop of making requests to the server
func startClient(clientID int, rng *rand.Rand) {
	client := &http.Client{}

	for {
		route := fmt.Sprintf("/route%d", rng.Intn(3)+1)
		url := "http://localhost:8080" + route

		// Sending request to the server
		resp, err := client.Get(url)
		if err != nil {
			fmt.Printf("Client %d Error: %v\n", clientID, err)
			continue
		}
		resp.Body.Close()

		longString, _ := generateRandomString(1024 * 50) // 50 Kb
		msg := fmt.Sprintf(`{"client": "%d", "route": "%s", "long_string": "%s"}`, clientID, url, longString)

		if err := wal.Write([]byte(msg)); err != nil {
			fmt.Println(err)
			return // Stop the client
		}

		fmt.Printf("Client %d Requested: %s\n", clientID, url)
		time.Sleep(1 * time.Second) // Throttle requests
	}
}

// helper function to simply simulate a long string
func generateRandomString(n int) (string, error) {
	b := make([]byte, n)
	_, err := rand.Read(b)
	if err != nil {
		return "", err
	}

	return base64.StdEncoding.EncodeToString(b), nil
}

func myCallback(entries []*xwalpb.WALEntry) error {
	for _, entry := range entries {
		fmt.Printf("Replaying entry: %d\n", entry.GetLSN())
	}
	return nil
}

func main() {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	// Initialize the write-ahead log
	cfg := xwal.NewXWALConfig("")
	cfg.BackendConfig.LocalFS.SegmentsFileSize = 5 // Reduce segments file size to 5 Mb so we can see Segments Files rotation in action
	var err error
	if wal, err = xwal.NewXWAL(cfg); err != nil {
		panic(err)
	}
	defer wal.Close()

	startServer()
	time.Sleep(1 * time.Second) // Give the server a moment to start
	fmt.Println("Server started. Press Ctrl+C to stop.")

	var wg sync.WaitGroup

	wg.Add(NumberOfClients)
	for i := 1; i <= NumberOfClients; i++ {
		go func(clientID int, rng *rand.Rand) {
			defer wg.Done()
			startClient(clientID, rng)
		}(i, rng)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			if wal.IsClosed() {
				return
			}

			time.Sleep(ReplayFrequency)

			if err := wal.Replay(myCallback, 5, false); err != nil {
				fmt.Println(err)
				return
			}
		}
	}()

	wg.Wait()
	fmt.Println("Exiting localfs example code..")
}
