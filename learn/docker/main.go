package main

import (
	"fmt"
	"os"
	"os/exec"
)

func main() {
	cmd := exec.Command("docker", "run", "--rm", "--name", "minik8s-learn",
		"busybox:1.36", "sh", "-c", "echo hello from a container; sleep 2; exit 3")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	err := cmd.Start()
	if err != nil {
		fmt.Println("could not start:", err)
		return
	}
	fmt.Println("started docker with PID", cmd.Process.Pid)

	err = cmd.Wait()
	if err != nil {
		fmt.Println("container failed:", err)
		return
	}
	fmt.Println("container succeeded")
}
