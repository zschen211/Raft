package raft

import "sync"
import "time"
import "math/rand"

func main() {

	count := 0
	finished := 0
	var mu sync.Mutex
	cond := sync.NewCond(&mu)

	for i := 0; i < 9; i++ {
		go func(x int) {
			vote := requestVote(x)
			mu.Lock()
			defer mu.Unlock()
			if vote {
				count++
			}
			finished++
			cond.Broadcast()
		}(i)
	}

	mu.Lock()
	for count < 5 && finished != 9 {
		cond.Wait()
	}
	if count >= 5 {
		println("received 5+ votes!")
	} else {
		println("lost")
	}
	mu.Unlock()
}

//Dummy function that waits for some time and grants vote
//based on whether the argument is odd or even.
func requestVote(idx int) bool {
	time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
	return idx%2 == 0
}
