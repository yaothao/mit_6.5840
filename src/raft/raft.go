package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
// create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
// start agreement on a new log entry
// rf.GetState() (term, isLeader)
// ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
// each time a new entry is committed to the log, each Raft peer
// should send an ApplyMsg to the service (or tester)
// in the same server.
//

import (
	// "bytes"

	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
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
	Command interface{}
	Term    int
	Index   int
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
	heartTimer      *time.Timer
	voteFor         int
	totalPeers      int
	applyChan       chan ApplyMsg

	voteReceived int
	majorityNum  int
	leaderId     int

	logs        []LogEntry
	commitIndex int
	lastApplied int
	// only for leader, reinitialize after every leader selection
	nextIndex  []int
	matchIndex []int
	// countLogAck map[int]int

	// Part 2D
	lastIncludedIndex int
	lastIncludedTerm  int
	snapShot          []byte
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(rf.currentTerm) != nil ||
		e.Encode(rf.voteFor) != nil || e.Encode(rf.logs) != nil || e.Encode(rf.lastIncludedIndex) != nil || e.Encode(rf.lastIncludedTerm) != nil {
		panic("failed to encode raft persistent state")
	}
	data := w.Bytes()
	if len(rf.snapShot) != 0 {
		rf.persister.Save(data, rf.snapShot)
		return
	}
	rf.persister.Save(data, nil)

}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var lastIncludedIndex int
	// DPrintf("server %v BEOFRE back up from persist with last included index %v\n", rf.me, rf.lastIncludedIndex)
	if d.Decode(&rf.currentTerm) != nil ||
		d.Decode(&rf.voteFor) != nil || d.Decode(&rf.logs) != nil || d.Decode(&lastIncludedIndex) != nil || d.Decode(&rf.lastIncludedTerm) != nil {
		panic("failed to decode raft persistent state")
	}
	rf.lastIncludedIndex = lastIncludedIndex
	DPrintf("server %v back up from persist with last included index %v\n", rf.me, rf.lastIncludedIndex)
}

func (rf *Raft) readSnapShot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	rf.snapShot = data
}
func (rf *Raft) realToVirtual(rIdx int) int {
	return rIdx + rf.lastIncludedIndex
}

func (rf *Raft) virtualToReal(vIdx int) int {
	return vIdx - rf.lastIncludedIndex
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	ridx := rf.virtualToReal(index)
	rf.lastIncludedTerm = rf.logs[ridx].Term
	rf.logs = rf.logs[ridx:]
	rf.lastIncludedIndex = index
	rf.snapShot = snapshot
	if rf.lastApplied < index {
		rf.lastApplied = index
	}
	DPrintf("server %v snapshot up to index %v\n", rf.me, rf.lastIncludedIndex)
	rf.persist()
	rf.mu.Unlock()

}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
	FirstCmd          LogEntry
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshotRPC(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	/*
		1. Reply immediately if term < currentTerm
		2. Create new snapshot file if first chunk (offset is 0)
		3. Write data into snapshot file at given offset
		4. Reply and wait for more data chunks if done is false
		5. Save snapshot file, discard any existing or partial snapshot with a smaller index
		6. If existing log entry has same index and term as snapshot’s last included entry, retain log entries following it and reply
		7. Discard the entire log
		8. Reset state machine using snapshot contents (and load snapshot’s cluster configuration)
	*/
	rf.mu.Lock()

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}

	vCurrLastIndex := rf.realToVirtual(len(rf.logs) - 1)
	rLastIncludedIndex := rf.virtualToReal(args.LastIncludedIndex)
	if vCurrLastIndex >= args.LastIncludedIndex && rf.logs[rLastIncludedIndex].Term == args.LastIncludedTerm {
		rf.logs = rf.logs[rLastIncludedIndex:]
	} else {
		rf.logs = rf.logs[:0]
		rf.logs = append([]LogEntry{args.FirstCmd}, rf.logs...)
	}

	rf.snapShot = args.Data
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.leaderId = args.LeaderId
	snapshotMsg := ApplyMsg{}
	snapshotMsg.Snapshot = rf.snapShot
	snapshotMsg.SnapshotIndex = rf.lastIncludedIndex
	snapshotMsg.SnapshotTerm = rf.lastIncludedTerm
	snapshotMsg.SnapshotValid = true
	rf.mu.Unlock()

	rf.applyChan <- snapshotMsg

	rf.mu.Lock()
	rf.commitIndex = args.LastIncludedIndex
	rf.lastApplied = args.LastIncludedIndex
	rf.mu.Unlock()
}
func (rf *Raft) InstallSnapshotRPCHandler(server int, args *InstallSnapshotArgs) {
	reply := &InstallSnapshotReply{}
	ok := rf.peers[server].Call("Raft.InstallSnapshotRPC", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		if reply.Term > args.Term {
			rf.switchToFollower(reply.Term, true)
			return
		}

		rf.matchIndex[server] = args.LastIncludedIndex
		rf.nextIndex[server] = rf.matchIndex[server] + 1
	}
}
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// fmt.Printf("Server is Killed %v\n", rf.me)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	ReceivedTerm int
	VoteGranted  bool
	Reject       bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	Reject  bool

	XTerm   int
	XIndex  int
	XLength int
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

	/*
	   Your code here (2B).
	   if the raft instance is killed,
	   return false with out acquiring locks
	*/

	if rf.killed() {
		return -1, -1, false
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	/* Return false if the current server is not leader */
	if isLeader = rf.role == Leader; !isLeader {
		return index, term, isLeader
	}

	index = rf.realToVirtual(len(rf.logs))
	term = rf.currentTerm

	newLog := LogEntry{command, term, index}
	DPrintf("leader %v : new logEntry %v\n", rf.me, newLog)
	/* Add command to current leader server's log */
	rf.logs = append(rf.logs, newLog)
	rf.resetHeartBeatTimer(10)
	rf.persist()
	return index, term, isLeader
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

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

	/*
	   The following code implements the election restriction:
	   a candidate need to be as lest as up-to-date as the
	   follower to be granted vote for leader
	*/
	currentLastIndex := rf.realToVirtual(len(rf.logs) - 1)
	currentLastTerm := rf.logs[len(rf.logs)-1].Term
	// var currentLastTerm int
	// if currentLastTerm = 0; currentLastIndex > 0 {
	// 	currentLastTerm = rf.logs[currentLastIndex-1].Term
	// }
	if rf.voteFor != -1 && rf.voteFor != args.CandidateId {
		reply.VoteGranted = false
	} else if args.LastLogTerm < currentLastTerm || (args.LastLogTerm == currentLastTerm && args.LastLogIndex < currentLastIndex) {
		reply.VoteGranted = false
	} else {
		reply.VoteGranted = true
		rf.voteFor = args.CandidateId
		rf.persist()
		rf.electionTimeOut = time.Now()
	}
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

	rf.voteFor = rf.me
	rf.voteReceived++
	rf.electionTimeOut = time.Now()

	for server := 0; server < rf.totalPeers; server++ {
		if server != rf.me {
			go func(server int) {

				args := RequestVoteArgs{}
				reply := RequestVoteReply{}

				rf.mu.Lock()
				args.CandidateId = rf.me
				// !! Need virtual index
				args.LastLogIndex = rf.realToVirtual(len(rf.logs) - 1)
				if len(rf.logs) == 1 {
					args.LastLogTerm = rf.lastIncludedTerm
				} else {

					args.LastLogTerm = rf.logs[len(rf.logs)-1].Term
				}

				// if args.LastLogTerm = 0; len(rf.logs) > 0 {
				// 	args.LastLogTerm = rf.logs[len(rf.logs)-1].Term
				// }
				args.Term = rf.currentTerm
				rf.mu.Unlock()

				rf.peers[server].Call("Raft.RequestVote", &args, &reply)
				rf.mu.Lock()
				if reply.Reject {
					rf.switchToFollower(reply.ReceivedTerm, true)
					rf.cond.Broadcast()
				} else if reply.VoteGranted {
					rf.voteReceived++
					rf.cond.Broadcast()
				}
				rf.mu.Unlock()
			}(server)
		}
	}

	for rf.role == Candidate && rf.voteReceived < rf.majorityNum {
		rf.cond.Wait()
	}
	if rf.role != Candidate {
		rf.mu.Unlock()
		return
	}
	if rf.voteReceived >= rf.majorityNum {
		go rf.heartBeat()
		rf.switchToLeader()
	}

	rf.mu.Unlock()
}

/*
Binary Search for the first > or = to target Term in the current raft logs
*/
func (rf *Raft) binaryFindFirstLog(left int, right int, target int) int {
	for left < right {
		mid := (left + right) / 2
		tempTerm := rf.logs[mid].Term
		if tempTerm >= target {
			right = mid
		} else {
			left = mid + 1
		}
	}
	return left
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Reject = true
		return
	}
	/*
	   Following Section act as a timer reset on election timeout
	*/
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
	reply.Term = rf.currentTerm

	/*
	   Case one when leader's record of follower exceed actual log OR log doesn’t contain an entry at prev Log Index whose term matches prevLogTerm (&5.3)
	*/
	// if len(args.Entries) == 0 {
	// 	// 心跳函数
	// 	DPrintf("server %v 接收到 leader %v 的心跳, PrevLogTerm=%v, PrevLogIndex=%v, len(Entries) = %v\n", rf.me, args.LeaderId, args.PrevLogTerm, args.PrevLogIndex, len(args.Entries))
	// } else {
	// 	DPrintf("server %v 收到 leader %v 的AppendEntries, PrevLogTerm=%v, PrevLogIndex=%v, len(Entries)= %+v \n", rf.me, args.LeaderId, args.PrevLogTerm, args.PrevLogIndex, len(args.Entries))
	// }

	/*
		Optimization on AppendEntry rejection (2C)
		In case of rejection:
		@Return XTerm: the conflicting Term at the Server side if any
				XIndex: the first index of XTerm (true entry index)
				XLength: the length of the current log (true entry size)
	*/
	rPrevLogIdx := rf.virtualToReal(args.PrevLogIndex)
	if args.PrevLogIndex > rf.lastIncludedIndex { // make sure prevlogindex is still in the log
		if len(rf.logs) <= rPrevLogIdx {
			reply.Success = false
			reply.XTerm = 0
			reply.XIndex = 0 + rf.lastIncludedIndex
			reply.XLength = rf.realToVirtual(len(rf.logs))
			return
		} else if rf.logs[rPrevLogIdx].Term != args.PrevLogTerm {
			reply.Success = false
			reply.XTerm = rf.logs[rPrevLogIdx].Term
			reply.XIndex = rf.realToVirtual(rf.binaryFindFirstLog(0, rPrevLogIdx, reply.XTerm))
			reply.XLength = rf.realToVirtual(len(rf.logs))
			DPrintf("server %v return <XTerm : %v> <XIndex : %v>\n", rf.me, reply.XTerm, reply.XIndex)
			return
		}
	} else if args.PrevLogIndex < rf.lastIncludedIndex {
		fmt.Printf("Error server %v [doesn't have prevlogindex in current logs]\n", rf.me)
	}

	/*
		If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (&5.3)
		通过遍历Entries，并且检查上述条件，有效防止重复AppendEntry 以及乱序AppendEntry
	*/
	for i, log := range args.Entries {
		ridx := rPrevLogIdx + i + 1
		if ridx < len(rf.logs) && rf.logs[ridx].Term != log.Term {
			rf.logs = rf.logs[:ridx]
			rf.logs = append(rf.logs, args.Entries[i:]...)
			break
		} else if ridx == len(rf.logs) {
			rf.logs = append(rf.logs, args.Entries[i:]...)
			break
		}
	}

	rf.persist()

	// if len(args.Entries) > 0 {
	// 	DPrintf("raft %v : appended to log %v\n", rf.me, rf.logs)
	// }

	reply.Success = true

	/*
	   Following Condition checks on whether leader heartbeat or
	   regular AppendE calls updates the leader's commitIndex.
	   If so, update the commitIndex of current state machine
	*/
	if args.LeaderCommit > rf.commitIndex {
		/*
		   The implementation of AppendE No.5 in figure 2 of Raft paper
		*/
		if rf.commitIndex = args.LeaderCommit; args.LeaderCommit > rf.realToVirtual(len(rf.logs)) {
			rf.commitIndex = rf.realToVirtual(len(rf.logs))
		}

		rf.cond.Broadcast()
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) (int, bool, bool, int, int, int) {
	// mt.Printf("Error not in send Append\n")
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		return reply.Term, reply.Success, true, reply.XTerm, reply.XIndex, reply.XLength
	}
	return reply.Term, reply.Success, false, reply.XTerm, reply.XIndex, reply.XLength
}

func (rf *Raft) resetHeartBeatTimer(timeStamp int) {
	rf.heartTimer.Reset(time.Duration(timeStamp) * time.Millisecond)
}

func (rf *Raft) heartBeat() {
	for !rf.killed() {
		<-rf.heartTimer.C
		rf.mu.Lock()
		/* break heartBeat if not leader anymore */
		if rf.role != Leader {
			rf.mu.Unlock()
			return
		}

		for i := 0; i < rf.totalPeers; i++ {
			if i != rf.me {
				startingCommandIndex := rf.nextIndex[i]
				// follower is too out dated, send install snapshot instead of regular AppendEntry
				if startingCommandIndex <= rf.lastIncludedIndex {
					args := InstallSnapshotArgs{}
					args.Term = rf.currentTerm
					args.LeaderId = rf.me
					args.LastIncludedIndex = rf.lastIncludedIndex
					args.LastIncludedTerm = rf.lastIncludedTerm
					args.Data = rf.snapShot
					args.FirstCmd = LogEntry{Command: rf.logs[0].Command, Term: rf.lastIncludedTerm, Index: rf.lastIncludedIndex}
					go rf.InstallSnapshotRPCHandler(i, &args)
					continue
				}
				args := AppendEntriesArgs{}
				reply := AppendEntriesReply{}

				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				args.PrevLogIndex = startingCommandIndex - 1
				args.PrevLogTerm = rf.logs[rf.virtualToReal(args.PrevLogIndex)].Term
				if args.PrevLogTerm == 0 && rf.lastIncludedTerm != 0 {
					args.PrevLogTerm = rf.lastIncludedTerm
				}
				args.LeaderCommit = rf.commitIndex

				/* condition check if current log has new op to send */
				if rf.realToVirtual(len(rf.logs)-1) >= rf.nextIndex[i] {
					args.Entries = rf.logs[rf.virtualToReal(startingCommandIndex):]
					// rf.appendWorkerState[i] = true

				}
				// DPrintf("leader %v 发送heartbeat给 %v entries 长度为")
				go rf.heartBeatHandler(i, &args, &reply)
			}
		}
		rf.mu.Unlock()

		/* send heart beat to follower every 100 ms */
		//time.Sleep(time.Duration(100) * time.Millisecond)
		rf.resetHeartBeatTimer(100)
	}
}

func (rf *Raft) heartBeatHandler(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	/* send AppendE RPC, followerAck representes whether follower replicated the logs */
	followerTerm, followerAck, connected, XTerm, XIndex, XLength := rf.sendAppendEntries(server, args, reply)
	/*
		case 1 when leader's term is outdated, immediately switch to follower
		case 2 when follower successful replicated the log entries
		case 3 whtn follower return false (check if raft is still leader with up-to-date term), decrement the nextInt
	*/
	rf.mu.Lock()
	if connected {
		/* No longer the leader */
		if followerTerm > rf.currentTerm {
			rf.mu.Unlock()
			rf.switchToFollower(followerTerm, true)
			return
		} else if followerAck {
			rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
			rf.nextIndex[server] = rf.matchIndex[server] + 1

			/*
				rN represent the possible real log index
				N represent the possible true log index
			*/
			rN := len(rf.logs) - 1
			N := rf.realToVirtual(rN)
			for N > rf.commitIndex {
				count := 1 // 1表示包括了leader自己
				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}
					if rf.matchIndex[i] >= N && rf.logs[rN].Term == rf.currentTerm {
						count += 1
					}
				}
				if count > rf.totalPeers/2 {
					// 如果至少一半的follower回复了成功, 更新commitIndex
					rf.commitIndex = N
					//DPrintf("raft %v : leader commit <index : %v> <log : %v>\n", rf.me, rf.commitIndex, rf.logs[rf.commitIndex])
					rf.cond.Broadcast()
					break
				}
				N -= 1
			}

		} else if reply.Term == rf.currentTerm && rf.role == Leader {
			/*
				Case 1: follower's log is too short:
				    nextIndex = XLen
				Case 2: leader has XTerm:
				    nextIndex = leader's last entry for XTerm
				Case 3: leader doesn't have XTerm:
					nextIndex = XIndex
			*/
			rPrevLogIndex := rf.virtualToReal(args.PrevLogIndex)
			if XTerm == 0 && XLength != 0 {
				rf.nextIndex[server] = XLength
			} else if XTerm > 0 {
				rIdx := rf.binaryFindFirstLog(0, rPrevLogIndex, XTerm)

				if rf.logs[rIdx].Term == XTerm {
					rEndIdx := rf.binaryFindFirstLog(rIdx, rPrevLogIndex, XTerm+1)
					rf.nextIndex[server] = rf.realToVirtual(rEndIdx - 1)
				} else {
					rf.nextIndex[server] = XIndex
				}
			}

		}
	}
	// rf.appendWorkerState[server] = false
	rf.mu.Unlock()
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		/* Condition Check : if in this code chunk, serve is in candidate state */
		rf.mu.Lock()
		if rf.role != Leader {
			etms := 300 + (rand.Int63() % 500)
			if time.Now().After(rf.electionTimeOut.Add(time.Duration(etms) * time.Millisecond)) {
				rf.switchToCandidate()
				go rf.selecLeader()
			}
		}

		/* pause for a random amount of time between 50 and 350 milliseconds. */
		rf.mu.Unlock()
		ms := 300 + (rand.Int63() % 500)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

/*
Function serves as a long running goroutine.
Applies command that is committed to state machine.
Sleep on condition commitIndex <= lastApplied.
Long running goroutine also check whether current
state machine is terminated.
*/
func (rf *Raft) applyCommand() {
	for !rf.killed() {
		rf.mu.Lock()

		/*
		   Condition checks that any newly commited logs
		*/
		for rf.commitIndex <= rf.lastApplied {
			rf.cond.Wait()
		}

		if rf.killed() {
			rf.mu.Unlock()
			return
		}
		/*
			Check whether the current replicated log contains available up-to-date logs
			otherwise set the latest commitIndex to last log entry
		*/
		vLogIdx := rf.realToVirtual(len(rf.logs) - 1)
		if rf.commitIndex > vLogIdx {
			rf.commitIndex = rf.realToVirtual(len(rf.logs) - 1)
		}

		rCommitIdx := rf.virtualToReal(rf.commitIndex)
		rApplyIdx := rf.virtualToReal(rf.lastApplied)
		cmd := make([]*ApplyMsg, 0)
		/*
			Apply all the available committed logs to server
		*/
		for tempIndex := rApplyIdx + 1; tempIndex <= rCommitIdx; tempIndex++ {

			applyMsg := &ApplyMsg{}
			msgPackage := rf.logs[tempIndex]
			applyMsg.Command = msgPackage.Command
			applyMsg.CommandIndex = msgPackage.Index
			// if applyMsg.CommandIndex != tempIndex+rf.lastIncludedIndex {
			// 	fmt.Printf("Error : Server %v apply msg Wrong Index\n <Expected : %v> <Received : %v>", rf.me, tempIndex+rf.lastIncludedIndex, applyMsg.CommandIndex)
			// }
			applyMsg.CommandValid = true
			// if len(rf.snapShot) != 0 {
			// 	applyMsg.Snapshot = rf.snapShot
			// 	applyMsg.SnapshotIndex = rf.lastIncludedIndex
			// 	applyMsg.SnapshotTerm = rf.lastIncludedTerm
			// 	applyMsg.SnapshotValid = true
			// }
			//DPrintf("raft %v : send to channel <%v, %v>\n", rf.me, applyMsg.CommandIndex, applyMsg.Command)

			cmd = append(cmd, applyMsg)

			//DPrintf("raft %v : apply %v <temp : %v> <rcommit : %v>\n", rf.me, applyMsg, tempIndex, rCommitIdx)
		}
		rf.mu.Unlock()

		for _, msg := range cmd {
			rf.mu.Lock()
			if msg.CommandIndex != rf.lastApplied+1 {
				rf.mu.Unlock()
				break
			}
			rf.mu.Unlock()
			DPrintf("raft %v : send to channel <%v, %v>\n", rf.me, msg.CommandIndex, msg.Command)
			rf.applyChan <- *msg

			rf.mu.Lock()
			if msg.CommandIndex != rf.lastApplied+1 {
				rf.mu.Unlock()
				break
			}
			rf.lastApplied = msg.CommandIndex
			rf.mu.Unlock()
		}
	}
}

/*
Method refresh the role state of the
server as a followerin the new term.
*/
func (rf *Raft) refreshTermState() {
	rf.voteFor = -1
	rf.voteReceived = 0
	rf.leaderId = -1
	rf.nextIndex = make([]int, rf.totalPeers)
	rf.matchIndex = make([]int, rf.totalPeers)
}

func (rf *Raft) switchToFollower(setToTerm int, refreshState bool) {
	rf.role = Follower
	rf.currentTerm = setToTerm
	if refreshState {
		rf.refreshTermState()
	}
	rf.persist()
	rf.electionTimeOut = time.Now()
}

func (rf *Raft) switchToCandidate() {
	rf.role = Candidate
	rf.currentTerm++
	rf.refreshTermState()
	rf.persist()
	rf.electionTimeOut = time.Now()
}

func (rf *Raft) switchToLeader() {
	rf.role = Leader

	/* Leader state initialization for log synchronization */
	for server := 0; server < rf.totalPeers; server++ {
		rf.nextIndex[server] = rf.realToVirtual(len(rf.logs))
		rf.matchIndex[server] = rf.lastIncludedIndex
	}
	//rf.countLogAck = make(map[int]int)
	//rf.appendWorkerState = make([]bool, rf.totalPeers)
	rf.persist()
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
	rf.totalPeers = len(rf.peers)
	rf.applyChan = applyCh

	rf.cond = *sync.NewCond(&rf.mu)
	rf.role = Follower
	rf.currentTerm = 0
	rf.voteFor = -1

	rf.majorityNum = (rf.totalPeers / 2) + 1
	rf.logs = make([]LogEntry, 0)
	rf.logs = append(rf.logs, LogEntry{Term: 0})

	rf.nextIndex = make([]int, rf.totalPeers)
	rf.matchIndex = make([]int, rf.totalPeers)
	rf.electionTimeOut = time.Now()
	rf.heartTimer = time.NewTimer(0)

	// rf.commitIndex = 0
	// rf.lastApplied = 0
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0
	rf.snapShot = make([]byte, 0)
	/*
		TODO: Upon re-start after crash
		Initialize state from snapshot
	*/

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.readSnapShot(persister.ReadSnapshot())

	rf.commitIndex = rf.lastIncludedIndex
	rf.lastApplied = rf.lastIncludedIndex

	/*
	   Start the goroutine to wait for any newly committed logs
	*/
	go rf.applyCommand()

	/*
	   start ticker goroutine that monitors election timeout
	*/
	go rf.ticker()
	rf.electionTimeOut = time.Now()

	return rf
}
