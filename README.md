# Notes
## labrpc
Call() is goroutine safe, use go net.Call() (Order may change).

Call() and server handlers must accept pointers of Args.

MakeService(***object***) where ***object*** is the server object.

## raft

First letter of fields in **RequestVoteArgs** and **RequestVoteReply** must be capitalized.

**Make** should start goroutine for heavy work.

*electionTimeout*? 400ms?

*heart beat interval* at most 100ms -> 200ms?

Raft lock?