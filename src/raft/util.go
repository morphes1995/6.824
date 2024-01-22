package raft

import "log"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf("[raft]--"+format, a...)
	}
	return
}

const Debug2 = 1

func DPrintf2(format string, a ...interface{}) (n int, err error) {
	if Debug2 > 0 {
		log.Printf("[raft]--"+format, a...)
	}
	return
}
