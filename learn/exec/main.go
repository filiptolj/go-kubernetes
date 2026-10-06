package main

import (
	"fmt"
	"os"
	"os/exec"
)

func main() {
	cmd := exec.Command("sh", "-c", "echo starting; sleep 2; echo done; exit 3")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	err := cmd.Start()
	if err != nil {
		fmt.Println("could not start:", err)
		return
	}
	fmt.Println("started process with PID", cmd.Process.Pid)

	err = cmd.Wait()
	if err != nil {
		fmt.Println("process failed:", err)
		return
	}
	fmt.Println("process succeeded")
}
