package main

import (
	"fmt"
	"os"
)

var (
	// Git SHA Value will be set during build
	GitTagSha = "Git tag sha: Not provided (use ./build instead of go build)"
)

func printVersion() {
	fmt.Println(GitTagSha)
	os.Exit(0)
}

func getVersion() string {
	return GitTagSha
}
