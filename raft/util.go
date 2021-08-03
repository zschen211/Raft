package raft

import "log"

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

func DPrintln(a ...interface{}) {
	if Debug {
		log.Println(a...)
	}
}

func DPrint(a ...interface{}) {
	if Debug {
		log.Print(a...)
	}
}
