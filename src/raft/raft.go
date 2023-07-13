package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

func StableHeartbeatTimeout() time.Duration {
	return time.Duration(100) * time.Millisecond
}

func RandomizedElectionTimeout() time.Duration {
	sleep_time := rand.Intn(200) + 200
	return time.Duration(sleep_time) * time.Millisecond
}

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Role int

const (
	Leader    Role = 0
	Candidate Role = 1
	Follower  Role = 2
)

type Entry struct {
	Term    int
	Command interface{}
	Index   int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	current_term int     // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	voted_for    int     // candidateId that received vote in current term (or null if none)
	log          []Entry // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)

	commit_index int // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	last_applied int // index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	// Reinitialized after election
	next_index  []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	match_index []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	role Role

	electionTimer  *time.Timer
	heartbeatTimer *time.Timer

	applyCh          chan ApplyMsg
	last_reply_index int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.current_term
	isleader = rf.role == Leader
	rf.mu.Unlock()
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

type AppendEntriesArgs struct {
	LeaderTerm        int
	LeaderId          int
	PrevLogIndex      int
	PrevLogTerm       int
	LogEntries        []Entry
	LeaderCommitIndex int // leader’s commitIndex
}
type AppendEntriesReply struct {
	Term    int
	Success bool
}

// AppendEntries RPC
func (rf *Raft) AppendEntries(arg *AppendEntriesArgs, reply *AppendEntriesReply) {
	// fmt.Println("append entries from ", arg.LeaderId)
	rf.mu.Lock()
	rf.electionTimer.Reset(RandomizedElectionTimeout())

	if arg.LeaderTerm < rf.current_term {
		reply.Term = rf.current_term
		reply.Success = false
		rf.mu.Unlock()
		return
	}

	// arg.LeaderTerm >= rf.current_term goes here
	// add more condition
	// TODO: 检查越界
	// empty LogEntries for heartbeat
	if len(arg.LogEntries) > 0 {
		// log没有那一项
		if arg.PrevLogIndex >= len(rf.log) {
			reply.Term = rf.current_term
			reply.Success = false
			rf.mu.Unlock()
			return
		}
		// log有那一项，但是不匹配
		if rf.log[arg.PrevLogIndex].Term != arg.PrevLogTerm {
			reply.Term = rf.current_term
			reply.Success = false
			rf.mu.Unlock()
			return
		}

		// from here rf.log[arg.PrevLogIndex].term == arg.PrevLogTerm, start copy log
		var pre_logs []Entry = rf.log[:arg.PrevLogIndex+1]
		rf.log = append(pre_logs, arg.LogEntries...)

		if arg.LeaderCommitIndex > rf.commit_index {
			if arg.LeaderCommitIndex > len(rf.log)-1 {
				rf.commit_index = len(rf.log) - 1
			} else {
				rf.commit_index = arg.LeaderCommitIndex
			}
		}

		for rf.last_applied < rf.commit_index {
			rf.last_applied++
			msg := ApplyMsg{CommandValid: true, CommandIndex: rf.log[rf.last_applied].Index, Command: rf.log[rf.last_applied].Command}
			rf.applyCh <- msg
			fmt.Println(rf.me, " apply ", msg)
		}
		fmt.Println(rf.me, " commit index ", rf.commit_index)
	} else {
		if arg.PrevLogIndex >= len(rf.log) {
			reply.Term = rf.current_term
			reply.Success = false
			rf.mu.Unlock()
			return
		}
		// log有那一项，但是不匹配
		if rf.log[arg.PrevLogIndex].Term != arg.PrevLogTerm {
			reply.Term = rf.current_term
			reply.Success = false
			rf.mu.Unlock()
			return
		}
		// 这里有bug，新的leader
		if arg.LeaderCommitIndex > rf.commit_index {
			if arg.LeaderCommitIndex > len(rf.log)-1 {
				rf.commit_index = len(rf.log) - 1
			} else {
				rf.commit_index = arg.LeaderCommitIndex
			}
		}

		for rf.last_applied < rf.commit_index {
			rf.last_applied++
			msg := ApplyMsg{CommandValid: true, CommandIndex: rf.log[rf.last_applied].Index, Command: rf.log[rf.last_applied].Command}
			rf.applyCh <- msg
			fmt.Println(rf.me, " apply ", msg)
		}
		// fmt.Println(rf.me, " commit index ", rf.commit_index)
	}

	// fmt.Println(rf.me, " commit index ", rf.commit_index)
	reply.Success = true
	reply.Term = rf.current_term
	rf.current_term = arg.LeaderTerm
	rf.role = Follower
	rf.voted_for = -1
	rf.mu.Unlock()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	CandidateTerm int
	CandidateId   int
	LastLogIndex  int
	LastLogTerm   int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	TermReply   int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()

	reply.VoteGranted = false

	if args.CandidateTerm < rf.current_term {
		reply.TermReply = rf.current_term
		reply.VoteGranted = false
		rf.mu.Unlock()
		return
	}
	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
	// 好像没说reply什么
	if args.CandidateTerm > rf.current_term {
		rf.role = Follower
		rf.current_term = args.CandidateTerm
		rf.voted_for = -1
		if rf.voted_for == -1 || args.CandidateId == rf.voted_for {
			log_last_term_of_me := rf.log[len(rf.log)-1].Term
			if args.LastLogTerm < log_last_term_of_me {
				reply.TermReply = rf.current_term
				reply.VoteGranted = false
				rf.mu.Unlock()
				return
			}
			if args.LastLogTerm == log_last_term_of_me && args.LastLogIndex < rf.log[len(rf.log)-1].Index {
				reply.TermReply = rf.current_term
				reply.VoteGranted = false
				rf.mu.Unlock()
				return
			}

			// vote for it
			reply.VoteGranted = true
			reply.TermReply = rf.current_term
			rf.voted_for = args.CandidateId
			// fmt.Println(rf.me, " vote for ", args.CandidateId)
			rf.mu.Unlock()
			rf.electionTimer.Reset(RandomizedElectionTimeout())
			return
		}
		reply.VoteGranted = true
		reply.TermReply = rf.current_term
		rf.voted_for = args.CandidateId
		// fmt.Println(rf.me, " vote1 for ", args.CandidateId)
		rf.mu.Unlock()
		rf.electionTimer.Reset(RandomizedElectionTimeout())

		return
	}
	// If the logs have last entries with different terms, then the log with the later term is more up-to-date.
	// If the logs end with the same term, then whichever log is longer is more up-to-date.
	if rf.voted_for == -1 || args.CandidateId == rf.voted_for {
		log_last_term_of_me := rf.log[len(rf.log)-1].Term
		if args.LastLogTerm < log_last_term_of_me {
			reply.TermReply = rf.current_term
			reply.VoteGranted = false
			rf.mu.Unlock()
			return
		}
		if args.LastLogTerm == log_last_term_of_me && args.LastLogIndex < rf.log[len(rf.log)-1].Index {
			reply.TermReply = rf.current_term
			reply.VoteGranted = false
			rf.mu.Unlock()
			return
		}

		// vote for it
		reply.VoteGranted = true
		reply.TermReply = rf.current_term
		rf.voted_for = args.CandidateId
		// fmt.Println(rf.me, " vote2 for ", args.CandidateId)
		rf.mu.Unlock()
		rf.electionTimer.Reset(RandomizedElectionTimeout())
		return
	}

	// already voted
	reply.TermReply = rf.current_term
	reply.VoteGranted = false
	rf.mu.Unlock()
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	term, isLeader = rf.GetState()
	if !isLeader {
		return index, term, isLeader
	}
	rf.mu.Lock()
	// append entries
	e := Entry{term, command, len(rf.log)}
	rf.log = append(rf.log, e)
	index = len(rf.log) - 1 // plus one for empty head
	fmt.Println("Start command", command, ", index ", index, ", term ", term, ", log ", rf.log, ", me ", rf.me)

	rf.mu.Unlock()

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) StartElection() {
	// 选举过程有时间限制
	sleep_time := rand.Intn(150) + 150
	rf.mu.Lock()
	rf.current_term += 1
	rf.voted_for = rf.me
	rf.mu.Unlock()
	var mu sync.Mutex
	cond := sync.NewCond(&mu)
	count := 0
	finished := 0
	time_out := false
	half := len(rf.peers) / 2
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		} else {
			go func(x int) {

				// send request vote rpc
				var args RequestVoteArgs
				var reply RequestVoteReply
				rf.mu.Lock()
				args.CandidateId = rf.me
				args.CandidateTerm = rf.current_term
				args.LastLogIndex = rf.log[len(rf.log)-1].Index
				args.LastLogTerm = rf.log[len(rf.log)-1].Term
				rf.mu.Unlock()

				ok := rf.sendRequestVote(x, &args, &reply)
				// fmt.Println(rf.me, "send request vote to ", x)
				mu.Lock()
				defer mu.Unlock()
				if ok {
					if reply.VoteGranted {
						count++
					} else if reply.TermReply > rf.current_term {
						rf.mu.Lock()
						rf.current_term = reply.TermReply
						rf.role = Follower
						rf.mu.Unlock()
					}
				}
				finished++
				cond.Broadcast()
			}(i)
		}
	}
	go func(sleep_time int) {
		time.Sleep(time.Duration(sleep_time) * time.Millisecond)
		mu.Lock()
		defer mu.Unlock()
		time_out = true
		cond.Broadcast()
	}(sleep_time)
	mu.Lock()
	for count < half && finished < len(rf.peers)-1 && !time_out && rf.role == Candidate {
		cond.Wait()
	}
	if time_out {
		fmt.Println(rf.me, " election timeout, re-elaction")
		return
	}
	if count >= half {
		fmt.Println(rf.me, " received ", count, " vote, more than half")
		rf.mu.Lock()
		rf.role = Leader
		// init nextIndex and matchIndex
		rf.next_index = make([]int, len(rf.peers))
		rf.match_index = make([]int, len(rf.peers))
		for i := 0; i < len(rf.peers); i++ {
			rf.next_index[i] = len(rf.log)
			rf.match_index[i] = 0
		}
		rf.mu.Unlock()
	} else {
		fmt.Println(rf.me, " reveived ", count, " vote, less than half")
		time.Sleep(time.Duration(sleep_time) * time.Millisecond)
	}
	mu.Unlock()
}

func (rf *Raft) RequestVoteTo(x int, count int, finished int, mu *sync.Mutex, cond *sync.Cond) {
	// send request vote rpc
	var args RequestVoteArgs
	var reply RequestVoteReply
	rf.mu.Lock()
	args.CandidateId = rf.me
	args.CandidateTerm = rf.current_term
	rf.mu.Unlock()
	// args.LastLogIndex
	// args.LastLogTerm
	ok := rf.sendRequestVote(x, &args, &reply)
	// fmt.Println(rf.me, "send request vote to ", x)
	mu.Lock()
	defer mu.Unlock()
	if ok && reply.VoteGranted {
		count++
	}
	finished++
	cond.Broadcast()
}

func (rf *Raft) StartApplyLogs() {
	// shoud hold lock
	rf.mu.Lock()
	var v = make([]int, 0)
	for i, x := range rf.match_index {
		if i == rf.me {
			continue
		}
		v = append(v, x)
	}
	sort.Ints(v)
	half := len(rf.peers) / 2
	update_index := v[half]
	if update_index > rf.commit_index && rf.log[update_index].Term == rf.current_term {
		fmt.Println("update_index", update_index)
		rf.commit_index = update_index
	}
	rf.mu.Unlock()
	for rf.last_applied < rf.commit_index {
		rf.last_applied++
		msg := ApplyMsg{}
		msg.CommandValid = true
		msg.CommandIndex = rf.log[rf.last_applied].Index
		msg.Command = rf.log[rf.last_applied].Command
		rf.applyCh <- msg
		fmt.Println("apply command", msg.Command, " index ", msg.CommandIndex)
	}

	fmt.Println(rf.me, " commit index ", rf.commit_index, ", last applied ", rf.last_applied)
}

func (rf *Raft) StartAppendEntriesOrHeastBeats() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(x int) {
			var args AppendEntriesArgs
			var reply AppendEntriesReply
			args.LeaderId = rf.me
			rf.mu.Lock()
			args.LeaderTerm = rf.current_term
			args.LeaderCommitIndex = rf.commit_index
			if rf.next_index[x]-1 < 0 {
				args.PrevLogIndex = 0
			} else {
				args.PrevLogIndex = rf.next_index[x] - 1
			}

			args.PrevLogTerm = rf.log[args.PrevLogIndex].Term

			var log_entries_num int = 0
			len_log := len(rf.log)
			send_log_entries := false

			// 什么时候该发log entries
			if rf.next_index[x] <= len_log-1 {
				// init other field according to rf.next_index and rf.match_index

				log_entries_num = len_log - rf.next_index[x]
				args.LogEntries = make([]Entry, log_entries_num)
				copied := copy(args.LogEntries, rf.log[rf.next_index[x]:])
				if log_entries_num != copied {
					fmt.Println("log_entries_num not equal to copied: ", log_entries_num, ":", copied)
					panic("error")
				}
				send_log_entries = true
				fmt.Println("send logs to ", x, " , logs ", args.LogEntries)
			}

			rf.mu.Unlock()
			ok := rf.sendAppendEntries(x, &args, &reply)
			if ok {
				fmt.Println(rf.me, " send heartbeat to ", x, ", reply success: ", reply.Success, ", its term: ", reply.Term)
				rf.mu.Lock()
				if reply.Term > rf.current_term {
					// here reply false
					rf.role = Follower
					rf.current_term = reply.Term
					rf.voted_for = -1
					rf.mu.Unlock()
					fmt.Println(rf.me, " become follower...")
					return
				}
				// update next_index
				if reply.Success {
					if send_log_entries {
						rf.match_index[x] = len_log - 1
						rf.next_index[x] = rf.match_index[x] + 1
						fmt.Println(x, ": match_index ", rf.match_index[x], ", next_index ", rf.next_index[x])
					}

				} else {
					if send_log_entries {
						// 这里不能一直减，会小于零
						rf.next_index[x] -= 1
						fmt.Println(x, " next index decri ", rf.next_index[x])

					}
					// heartbeat reply false
				}
				rf.mu.Unlock()
			} else {
				// try more times
				fmt.Println("send heartbeat not ok")
			}
		}(i)
	}

	go rf.StartApplyLogs()
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()
		switch rf.role {
		case Follower:
			rf.mu.Unlock()

			select {
			case <-rf.electionTimer.C:
				fmt.Println(rf.me, " have not heard from leader")
				rf.mu.Lock()
				rf.role = Candidate
				rf.mu.Unlock()
			}

		case Candidate:
			rf.mu.Unlock()
			// Candidate then request vote
			rf.StartElection()

		case Leader:
			rf.mu.Unlock()
			// Leader then send heartbeats
			rf.StartAppendEntriesOrHeastBeats()
			time.Sleep(time.Duration(100) * time.Millisecond)
		}

	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.current_term = 0 // 应该从文件中读取
	rf.voted_for = -1
	rf.commit_index = 0
	rf.last_applied = 0
	rf.role = Follower
	rf.electionTimer = time.NewTimer(RandomizedElectionTimeout())
	rf.heartbeatTimer = time.NewTimer(StableHeartbeatTimeout())
	rf.log = make([]Entry, 1)
	rf.applyCh = applyCh
	rf.last_reply_index = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
