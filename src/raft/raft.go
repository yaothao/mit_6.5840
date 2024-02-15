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

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

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

type RaftRole int

const (
	Follower RaftRole = iota
	Candidate
	Leader
)

type LogEntry struct {
	command interface{}
	term    int
	index   int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex // Lock to protect shared access to this peer's state
	cond      sync.Cond
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).

	role            RaftRole
	currentTerm     int
	electionTimeOut time.Time
	voteFor         int

	isLeaderActive bool
	voteReceived   int
	majorityNum    int
	leaderId       int

	logs        []LogEntry
	commitIndex int
	lastApplied int
	// only for leader
	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
	// fmt.Printf("I am in Get State for %v\n", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isLeader := rf.role == Leader
	// fmt.Printf("I am in Get State for %v, term: %v\n", rf.me, term)

	return term, isLeader

}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	//fmt.Printf("Server is Killed %v\n", rf.me)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term        int
	CandidateId int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	ReceivedTerm int
	VoteGranted  bool
	Reject       bool
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	Reject  bool
}

func (rf *Raft) refreshTermState() {
	rf.voteFor = -1
	rf.voteReceived = 0
	rf.leaderId = -1
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// fmt.Printf("%v Received request from %v in term%v, current Term: %v\n", rf.me, args.CandidateId, args.Term, rf.currentTerm)
	if args.Term > rf.currentTerm {
		preRole := rf.role
		rf.switchToFollower(args.Term, true)
		if preRole == Leader || preRole == Candidate {
			rf.cond.Broadcast()
		}
	}

	reply.ReceivedTerm = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Reject = true
		return
	}
	//fmt.Printf("%v Received request from %v in term%v\n", rf.me, args.CandidateId, rf.currentTerm)
	if rf.voteFor != -1 && rf.voteFor != args.CandidateId {
		reply.VoteGranted = false
	} else {
		reply.VoteGranted = true
		rf.voteFor = args.CandidateId
		rf.electionTimeOut = time.Now()
	}
	//fmt.Printf("Release lock in Request Vote %v\n", rf.me)

}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//fmt.Printf("Acquire lock in Append Entries %v\n", rf.me)

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Reject = true
		return
	} else if args.Term > rf.currentTerm || true {
		preRole := rf.role
		if args.Term > rf.currentTerm {
			rf.switchToFollower(args.Term, true)
		} else {
			rf.switchToFollower(args.Term, false)
		}
		rf.leaderId = args.LeaderId
		if preRole == Candidate {
			rf.cond.Broadcast()
		}

	}

	reply.Term = rf.currentTerm
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//mt.Printf("Error not in send Append\n")
	rf.peers[server].Call("Raft.AppendEntries", args, reply)
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

func (rf *Raft) selecLeader() {
	rf.mu.Lock()
	//fmt.Printf("Acquire lock in Select Leader Vote %v\n", rf.me)

	rf.voteFor = rf.me
	rf.voteReceived++
	rf.electionTimeOut = time.Now()

	for server := 0; server < len(rf.peers); server++ {
		if server != rf.me {
			go func(candidateId int, term int, server int) {

				args := RequestVoteArgs{CandidateId: candidateId, Term: term}
				reply := RequestVoteReply{}

				rf.peers[server].Call("Raft.RequestVote", &args, &reply)
				//fmt.Printf("Error not after send Request\n")
				rf.mu.Lock()
				if reply.Reject {
					rf.switchToFollower(reply.ReceivedTerm, true)
					rf.cond.Broadcast()
					// fmt.Printf("Candidate Server %v received Reject in Term %v\n", rf.me, rf.currentTerm)
				} else if reply.VoteGranted {
					rf.voteReceived++
					// fmt.Printf("Candidate Server %v received %v/%v Votes in Term %v\n", rf.me, rf.voteReceived, rf.majorityNum, rf.currentTerm)
					rf.cond.Broadcast()
				}
				rf.mu.Unlock()
			}(rf.me, rf.currentTerm, server)
		}
	}

	for rf.role == Candidate && rf.voteReceived < rf.majorityNum {
		//fmt.Printf("%v is not met yet\n", rf.voteReceived < rf.majorityNum)
		rf.cond.Wait()
	}
	if rf.role != Candidate {
		//fmt.Printf("Election ended for %v\n", rf.me)
		rf.mu.Unlock()
		return
	}
	if rf.voteReceived >= rf.majorityNum {
		go rf.heartBeat()
		rf.switchToLeader()
		// fmt.Printf("------- %v is leader -------\n", rf.me)
	}

	rf.mu.Unlock()

}

func (rf *Raft) heartBeat() {
	// fmt.Printf("Start Hear Beat\n")
	for rf.killed() == false {
		rf.mu.Lock()
		// break heartBeat if not leader anymore
		if rf.role != Leader {
			rf.mu.Unlock()
			return
		}
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				args := AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me}
				reply := AppendEntriesReply{}

				go func(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
					rf.sendAppendEntries(server, args, reply)
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if reply.Reject {
						// rf.currentTerm = reply.Term
						// rf.role = Follower
						rf.switchToFollower(reply.Term, true)
						// rf.refreshTermState()
						// fmt.Printf("%v server is revert to follower in term %v\n", rf.me, rf.currentTerm)
					}
				}(i, &args, &reply)
			}
		}
		rf.mu.Unlock()

		// send heart beat to follower every 150 ms
		time.Sleep(time.Duration(100) * time.Millisecond)
	}
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// if in this code chunk, serve is in candidate state
		rf.mu.Lock()
		if rf.role != Leader {

			//rf.isLeaderActive = !time.Now().After(rf.electionTimeOut.Add(150 * time.Millisecond))
			etms := 300 + (rand.Int63() % 500)
			if time.Now().After(rf.electionTimeOut.Add(time.Duration(etms) * time.Millisecond)) {
				// rf.role = Candidate
				// rf.currentTerm++
				rf.switchToCandidate()
				// fmt.Printf("Election initiated by %v, Term: %v\n", rf.me, rf.currentTerm)
				//rf.refreshTermState()
				go rf.selecLeader()
				//go rf.leaderSelectionListener()

			}
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		//fmt.Printf("I am releasing lock and going to sleep\n")
		rf.mu.Unlock()
		ms := 300 + (rand.Int63() % 500)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) switchToFollower(setToTerm int, refreshState bool) {
	rf.role = Follower
	rf.currentTerm = setToTerm
	if refreshState {
		rf.refreshTermState()
	}
	rf.electionTimeOut = time.Now()
}

func (rf *Raft) switchToCandidate() {
	rf.role = Candidate
	rf.currentTerm++
	rf.refreshTermState()
	rf.electionTimeOut = time.Now()
}

func (rf *Raft) switchToLeader() {
	rf.role = Leader
	rf.electionTimeOut = time.Now()
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

	rf.cond = *sync.NewCond(&rf.mu)
	rf.role = Follower
	rf.currentTerm = 0
	rf.voteFor = -1
	//rf.isLeaderActive = true
	rf.majorityNum = (len(rf.peers) / 2) + 1
	rf.electionTimeOut = time.Now()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	rf.electionTimeOut = time.Now()

	return rf
}
