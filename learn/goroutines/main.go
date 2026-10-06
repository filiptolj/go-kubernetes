package main

import (
	"fmt"
	"time"
)

// watchPods pretends to be the API server, sending an event for each new pod.
func watchPods(events chan<- string) {
	names := []string{"nginx", "reids", "postgress"}
	for _, name := range names {
		time.Sleep(time.Second)
		events <- "ADDED " + name
	}
	close(events)
}

func main() {
	events := make(chan string)
	go watchPods(events)

	for event := range events {
		fmt.Println("got event:", event)
	}
	fmt.Println("watch closed")
}
