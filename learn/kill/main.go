package main

import (
	"fmt"
	"os"
	"os/exec"
	"time"
)

func main() {
	cmd := exec.Command("sh", "-c", "echo starting; sleep 5; echo done")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	err := cmd.Start()
	if err != nil {
		fmt.Println("could not start:", err)
		return
	}
	fmt.Println("started process with PID", cmd.Process.Pid)

	done := make(chan error)
	go func() {
		done <- cmd.Wait()
	}()

	select {
	case err := <-done:
		fmt.Println("process exited by itself:", err)
	case <-time.After(1 * time.Second):
		fmt.Println("taking too long, killing it")
		cmd.Process.Kill()
		fmt.Println("result:", <-done)
	}
}
