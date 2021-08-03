package main

import (
	"sort"
	"sync"
)

// Timestamp ordering states
type TsoState struct {
	mu            sync.Mutex
	CommitVal     int
	CommitVersion string
	Rts           *[]string      // stores txID
	Tw            map[string]int // { txID: value }
}

func getTwKeys(account string) []string {
	var keys []string
	for k := range objState[account].Tw {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func printRts(account string) {
	for _, v := range *objState[account].Rts {
		println(v)
	}
}

func printTw(account string) {
	keys := getTwKeys(account)
	obj := objState[account].Tw
	for _, k := range keys {
		println(k, obj[k])
	}
}

func InitState() *TsoState {
	rts := make([]string, 0)
	tw := make(map[string]int)
	return &TsoState{CommitVal: 0, CommitVersion: "", Rts: &rts, Tw: tw}
}

func AddToRts(rts *[]string, txId string) {
	exist := false
	for _, v := range *rts {
		if v == txId {
			exist = true
			break
		}
	}
	if exist == false {
		(*rts) = append((*rts), txId)
		sort.Strings((*rts))
	}
}

func AddToTw(tw map[string]int, amount int, txId string) {
	tw[txId] = amount
}

func commitReady(commitObjs map[string]*TsoState, txId string) bool {
	commitReady := true
	for account, obj := range commitObjs {
		obj.mu.Lock()
		txs := getTwKeys(account)
		if txs[0] != txId {
			commitReady = false
		}
		obj.mu.Unlock()
	}
	return commitReady
}
