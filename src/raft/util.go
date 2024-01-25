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

func DPrint2(format string, a ...interface{}) (n int, err error) {
	log.Printf("[raft]--"+format, a...)
	return
}
