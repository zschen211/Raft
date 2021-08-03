package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isLeader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

// Enum for states
const (
	follower  int = 0
	leader    int = 1
	candidate int = 2
)
const electionTimeout = 700
const hbInterval = 200 * time.Millisecond

func getElectionTimeout() time.Duration {
	return time.Duration(rand.Intn(electionTimeout)+electionTimeout) * time.Millisecond
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	CandidateID  int
	VoteReqTerm  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term  int
	Agree bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
	Entries      []*LogEntry
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	Xterm   int
	Xindex  int
	Xlen    int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu    sync.Mutex          // Lock to protect shared access to this peer's state
	peers []*labrpc.ClientEnd // RPC end points of all peers
	me    int                 // this peer's index into peers[]
	dead  int32               // set by Kill()

	// Your data here (2A, 2B).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// You may also need to add other state, as per your implementation.
	majority        int
	curTerm         int
	curState        int
	votedFor        int
	electionTimeout time.Duration
	hbChan          chan bool

	applyCh     chan ApplyMsg
	log         []*LogEntry
	logmu       sync.Mutex
	commitIndex int
	lastApplied int
	nextIndex   []int
	nidxmu      sync.Mutex
	matchIndex  []int
	midxmu      sync.Mutex
}

func (rf *Raft) dPrintln(a ...interface{}) {
	DPrintf("Node %d: %s", rf.me, fmt.Sprintln(a...))
}

func (rf *Raft) dPrintf(f string, a ...interface{}) {
	DPrintf("Node %d: %s", rf.me, fmt.Sprintf(f, a...))
}

// Show all log entries for debugging purposes (Not thread-safe)
func (rf *Raft) dPrintLog() {
	DPrintf("############ Server %d's LOG ############", rf.me)
	for i, entry := range rf.log {
		DPrintln("entry", i+1, ":", entry)
	}
	DPrintf("#########################################")
}

// Show all nextIndex entries for debugging purposes (Not thread-safe)
func (rf *Raft) dPrintNextIndex() {
	DPrintf("######### Server %d's NextIndex #########", rf.me)
	for i, v := range rf.nextIndex {
		DPrintln("server", i, ":", v)
	}
	DPrintf("#########################################")
}

// Show all matchIndex entries for debugging purposes (Not thread-safe)
func (rf *Raft) dPrintMatchIndex() {
	DPrintf("######### Server %d's MatchIndex #########", rf.me)
	for i, v := range rf.matchIndex {
		DPrintln("server", i, ":", v)
	}
	DPrintf("##########################################")
}

func (rf *Raft) lock() {
	// _, _, num, _ := runtime.Caller(1)
	// rf.dPrintln("Locked by line", num)
	rf.mu.Lock()
}

func (rf *Raft) unlock() {
	//_, _, num, _ := runtime.Caller(1)
	//rf.dPrintln("Unlocked by line", num)
	rf.mu.Unlock()
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (term int, isLeader bool) {
	// Your code here (2A).
	rf.lock()
	defer rf.unlock()
	term = rf.curTerm
	isLeader = rf.curState == leader
	return
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	return rf.peers[server].Call("Raft.RequestVote", args, reply)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	//rf.dPrintln("HB sent to ", server)
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// Read the fields in "args",
	// and accordingly assign the values for fields in "reply".
	rf.lock()
	defer rf.unlock()
	reply.Term = rf.curTerm
	reply.Agree = false
	if args.VoteReqTerm < rf.curTerm {
		return
	} else if args.VoteReqTerm > rf.curTerm {
		// rf.dPrintf("VotedFor reset to null, curTerm %d, VoteTerm %d\n", rf.curTerm, args.VoteReqTerm)
		rf.curTerm = args.VoteReqTerm
		rf.votedFor = -1
	}
	rf.logmu.Lock()
	lastIndex := 0
	lastTerm := -1
	if len(rf.log) > 0 {
		lastIndex = rf.log[len(rf.log)-1].Index
		lastTerm = rf.log[len(rf.log)-1].Term
	}
	rf.logmu.Unlock()
	moreUpToDate := (lastTerm < args.LastLogTerm) || (lastTerm == args.LastLogTerm && lastIndex <= args.LastLogIndex)
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateID) && moreUpToDate {
		reply.Agree = true
		rf.votedFor = args.CandidateID
	}
}

// Receiver actions for handling AppendEntries RPCs
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.lock()
	reply.Term = rf.curTerm
	reply.Success = true
	if args.Term < rf.curTerm {
		// Reject RPC from lower term
		rf.unlock()
		return
	}
	if args.Term >= rf.curTerm {
		// Stale leadership or candidate, switch back to follower, update term
		rf.curState = follower
		rf.curTerm = args.Term
	}
	rf.unlock()
	// rf.dPrintf("Received AE (leaderID: %d, leaderTerm: %d) ", args.LeaderID, args.Term)
	rf.hbChan <- true
	//  Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	if args.PrevLogIndex >= 1 {
		rf.logmu.Lock()
		if len(rf.log) < args.PrevLogIndex {
			reply.Success = false
			reply.Xlen = len(rf.log)
			rf.logmu.Unlock()
			return
		} else if rf.log[args.PrevLogIndex-1].Term != args.PrevLogTerm {
			reply.Success = false
			reply.Xterm = rf.log[args.PrevLogIndex-1].Term
			for _, entry := range rf.log {
				if entry.Term == reply.Xterm {
					reply.Xindex = entry.Index
					break
				}
			}
			reply.Xlen = len(rf.log)
			rf.logmu.Unlock()
			return
		}
		rf.logmu.Unlock()
	}
	reply.Success = true
	// If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it
	if len(args.Entries) != 0 {
		rf.logmu.Lock()
		for _, entry := range args.Entries {
			entryIdx := entry.Index
			if entryIdx <= len(rf.log) {
				if rf.log[entryIdx-1].Term != entry.Term {
					rf.log = rf.log[0 : entryIdx-1]
					rf.log = append(rf.log, entry)
				}
			} else {
				// Append any new Entries not already in the log
				rf.log = append(rf.log, entry)
			}
		}
		// rf.dPrintf("LeaderID: %d, LeaderCommit: %d\n", args.LeaderID, args.LeaderCommit)
		rf.dPrintLog()
		rf.logmu.Unlock()
	}
	// If leaderCommit > commitIndex,
	// set commitIndex = min(leaderCommit, index of last new entry)
	rf.lock()
	if args.LeaderCommit > rf.commitIndex {
		rf.logmu.Lock()
		lastEntryIdx := len(rf.log)
		rf.logmu.Unlock()
		if args.LeaderCommit <= lastEntryIdx {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastEntryIdx
		}
	}
	rf.unlock()
	rf.commitEntries()
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
	// Your code here (2B).
	if rf.killed() {
		return
	}
	rf.lock()
	isLeader = rf.curState == leader
	rf.unlock()
	if !isLeader {
		return
	}

	rf.lock()
	rf.logmu.Lock()
	rf.midxmu.Lock()
	index = len(rf.log) + 1
	term = rf.curTerm
	rf.log = append(rf.log, &LogEntry{term, index, command})
	rf.dPrintLog()
	rf.matchIndex[rf.me] = index
	rf.midxmu.Unlock()
	rf.logmu.Unlock()
	rf.unlock()

	go rf.handleNewCommand(index)
	return
}

// self-append new entry & unicast it to all other servers
//
// If last log index ≥ nextIndex for a follower: send
// AppendEntries RPC with log entries starting at nextIndex
func (rf *Raft) handleNewCommand(lastLogIdx int) {
	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}
		go func(serverIdx int) {
			rf.nidxmu.Lock()
			decideSend := lastLogIdx >= rf.nextIndex[serverIdx]
			rf.nidxmu.Unlock()

			if decideSend {
				args := rf.makeAppendEntriesArgs(serverIdx)
				reply := &AppendEntriesReply{}
				// rf.dPrintf("Send idx %d to server %d\n", lastLogIdx, serverIdx)
				if ok := rf.sendAppendEntries(serverIdx, args, reply); ok {
					succeed := reply.Success
					if rf.backToFollower(reply.Term) {
						return
					}
					for !succeed {
						// If AppendEntries fails because of log inconsistency: decrement nextIndex and retry
						rf.nidxmu.Lock()
						if reply.Xterm == 0 {
							if reply.Xlen > 0 {
								rf.nextIndex[serverIdx] = reply.Xlen
							} else {
								rf.nextIndex[serverIdx] = 1
							}
						} else {
							exist := false
							rf.logmu.Lock()
							for _, entry := range rf.log {
								if entry.Term == reply.Xterm {
									exist = true
									rf.nextIndex[serverIdx] = entry.Index
								}
							}
							if !exist {
								rf.nextIndex[serverIdx] = reply.Xindex
							}
							rf.logmu.Unlock()
						}
						rf.nidxmu.Unlock()
						args = rf.makeAppendEntriesArgs(serverIdx)
						reply = &AppendEntriesReply{}
						if ok = rf.sendAppendEntries(serverIdx, args, reply); ok {
							if rf.backToFollower(reply.Term) {
								return
							}
							succeed = reply.Success
						}
					}
					// If successful: update nextIndex and matchIndex for follower
					if succeed {
						rf.updateIndices(serverIdx, lastLogIdx+1)
						rf.commitEntries()
					}
				}
			}
		}(idx)
	}
}

// generate Args for AppendEntries RPC
func (rf *Raft) makeAppendEntriesArgs(serverIdx int) *AppendEntriesArgs {
	rf.nidxmu.Lock()
	rf.logmu.Lock()
	prevLogIndex := rf.nextIndex[serverIdx] - 1
	prevLogTerm := -1
	if prevLogIndex >= 1 && prevLogIndex <= len(rf.log) {
		prevLogTerm = rf.log[prevLogIndex-1].Term
	}
	// rf.dPrintLog()
	entries := rf.log[rf.nextIndex[serverIdx]-1:]
	rf.logmu.Unlock()
	rf.nidxmu.Unlock()

	rf.lock()
	args := &AppendEntriesArgs{
		Term:         rf.curTerm,
		LeaderID:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		LeaderCommit: rf.commitIndex,
		Entries:      entries,
	}
	rf.unlock()

	return args
}

// helper function for updating nextIndex[] and matchIndex[] after AE succeeded
func (rf *Raft) updateIndices(serverIdx int, nextIdx int) {
	rf.nidxmu.Lock()
	rf.midxmu.Lock()
	if nextIdx > rf.nextIndex[serverIdx] {
		rf.nextIndex[serverIdx] = nextIdx
		rf.matchIndex[serverIdx] = rf.nextIndex[serverIdx] - 1
	}
	// rf.dPrintNextIndex()
	// rf.dPrintMatchIndex()
	rf.midxmu.Unlock()
	rf.nidxmu.Unlock()

	// If there exists an N such that
	// 1.N > commitIndex; 2.a majority of matchIndex[i] ≥ N; 3.log[N].term == currentTerm:
	// set commitIndex = N
	rf.lock()
	rf.logmu.Lock()
	for N := rf.commitIndex + 1; N <= len(rf.log); N++ {
		rf.midxmu.Lock()
		count := 0
		for _, v := range rf.matchIndex {
			if v >= N {
				count += 1
			}
		}
		rf.midxmu.Unlock()

		if count >= rf.majority && rf.log[N-1].Term == rf.curTerm {
			rf.commitIndex = N
			// rf.dPrintf("Commit index: %d\n", rf.commitIndex)
		}
	}
	rf.logmu.Unlock()
	rf.unlock()
}

// feed all committed log entries to the service (or client)
func (rf *Raft) commitEntries() {
	rf.lock()
	// rf.dPrintf("Commit idx: %d, lastApplied: %d\n", rf.commitIndex, rf.lastApplied)
	for rf.commitIndex > rf.lastApplied {
		rf.lastApplied += 1

		rf.logmu.Lock()
		applyMsg := &ApplyMsg{
			CommandValid: true,
			Command:      rf.log[rf.lastApplied-1].Command,
			CommandIndex: rf.log[rf.lastApplied-1].Index,
		}
		rf.logmu.Unlock()

		rf.applyCh <- *applyMsg
	}
	rf.unlock()
}

// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
func (rf *Raft) backToFollower(replyTerm int) bool {
	rf.lock()
	defer rf.unlock()
	// rf.dPrintf("replyTerm: %d, myTerm: %d\n", replyTerm, rf.curTerm)
	if replyTerm > rf.curTerm {
		rf.curTerm = replyTerm
		rf.curState = follower
		// rf.dPrintf("step down, myState: %d, myTerm: %d\n", rf.curState, rf.curTerm)
		return true
	}
	return false
}

func (rf *Raft) sendHB() {
	for {
		rf.lock()
		if rf.curState != leader {
			rf.unlock()
			return
		}
		rf.unlock()
		for idx := range rf.peers {
			if idx == rf.me {
				continue
			}
			args := rf.makeAppendEntriesArgs(idx)
			go rf.sendAppendEntries(idx, args, &AppendEntriesReply{})
		}

		time.Sleep(hbInterval)
	}
}

func (rf *Raft) elect(ch chan bool) {
	// When a AppendEntries with hb RPC is received (which means a leader has already been elected)
	// then this elect process should be terminated (ch <- leaderID) and rf's state should be changed
	// back to follower
	rf.lock()
	rf.curTerm++
	rf.curState = candidate
	rf.votedFor = rf.me
	// rf.dPrintln("Voted for itself")
	rf.unlock()
	count := 1
	finished := 1
	var mu sync.Mutex
	cond := sync.NewCond(&mu)
	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}
		go func(serIdx int) {
			reply := &RequestVoteReply{}
			rf.logmu.Lock()
			lastLogIndex := 0
			lastLogTerm := -1
			if len(rf.log) > 0 {
				lastLogIndex = rf.log[len(rf.log)-1].Index
				lastLogTerm = rf.log[len(rf.log)-1].Term
			}
			rf.logmu.Unlock()
			rf.lock()
			args := &RequestVoteArgs{
				VoteReqTerm:  rf.curTerm,
				CandidateID:  rf.me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			rf.unlock()
			if ok := rf.sendRequestVote(serIdx, args, reply); !ok {
				// rf.dPrintln(serIdx, "disconnected during vote request RPC")
				mu.Lock()
				finished++
			} else {
				mu.Lock()
				if reply.Agree {
					count++
				}
				finished++
			}
			mu.Unlock()
			cond.Broadcast()
		}(idx)
	}
	mu.Lock()
	defer mu.Unlock()
	for count < rf.majority && finished != len(rf.peers) {
		cond.Wait()
	}
	// rf.dPrintln("Cond fulfilled, agreed:", count, "finished", finished)
	if count >= rf.majority {
		rf.lock()
		rf.curState = leader
		rf.dPrintf("********** LEADER (Term %d) *********\n", rf.curTerm)
		rf.logmu.Lock()
		rf.nidxmu.Lock()
		rf.midxmu.Lock()
		for i := range rf.nextIndex {
			rf.nextIndex[i] = len(rf.log) + 1
			rf.matchIndex[i] = 0
		}
		rf.midxmu.Unlock()
		rf.nidxmu.Unlock()
		rf.logmu.Unlock()
		rf.unlock()
		// start sending heartbeats
		go rf.sendHB()
		ch <- true
	}
}

func (rf *Raft) startElection() {
	rf.lock()
	if rf.curState == candidate {
		// rf.dPrintln("Election is running")
		rf.unlock()
		return
	}
	rf.unlock()
	ch := make(chan bool)
	electionTimer := time.NewTimer(rf.electionTimeout)
	for {
		// rf.dPrintln("starting a new election run")
		go rf.elect(ch)
		select {
		case picked := <-ch:
			if picked {
				// rf.dPrintln("self is elected as leader")
				return
			}
		case <-rf.hbChan:
			return
		case <-electionTimer.C:
			// rf.dPrintln("election timeout, start a new elect run")
			electionTimer.Reset(rf.electionTimeout)
		}
	}
}

func (rf *Raft) background() {
	for {
		if rf.killed() {
			return
		}
		timer := time.NewTimer(rf.electionTimeout)
		select {
		case <-rf.hbChan:
			// rf.dPrintln("HB RPC received, reset timer")
		case <-timer.C:
			rf.lock()
			cond := rf.curState == follower
			rf.unlock()
			if cond {
				// rf.dPrintln("Timeout, kick off new election")
				rf.startElection()
				<-rf.hbChan // Waiting for the first hb RPC
			}
		}
	}
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	// TODO memory leak
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//w
func Make(peers []*labrpc.ClientEnd, me int, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:           peers,
		me:              me,
		majority:        (len(peers) + 1) / 2,
		votedFor:        -1,
		electionTimeout: getElectionTimeout(),
		hbChan:          make(chan bool),
		applyCh:         applyCh,
		log:             make([]*LogEntry, 0),
		nextIndex:       make([]int, len(peers)),
		matchIndex:      make([]int, len(peers)),
	}

	// Your initialization code here (2A, 2B).
	go rf.background()
	return rf
}
