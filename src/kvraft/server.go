package kvraft

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	OpCommand OpCommand
	Key       string
	Value     string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvStorage     map[string]string
	getChan       chan Op
	putAppendChan chan Op
}

func (kv *KVServer) checkIsLeader() bool {
	_, isLeader := kv.rf.GetState()
	return isLeader
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// if !kv.checkIsLeader() {
	// 	reply.Err = ErrWrongLeader
	// 	return
	// }

	newOp := Op{OpCommand: GetCommand, Key: args.Key}
	//fmt.Printf("server %v get 1\n", kv.me)
	kv.mu.Lock()
	//fmt.Printf("server %v get 2\n", kv.me)
	_, _, isLeader := kv.rf.Start(newOp)
	kv.mu.Unlock()
	//fmt.Printf("server %v get 3\n", kv.me)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	select {
	case receivedOp := <-kv.getChan:
		kv.mu.Lock()
		if val, ok := kv.kvStorage[receivedOp.Key]; ok {
			reply.Err = OK
			reply.Value = val
		} else {
			reply.Err = ErrNoKey
		}
		kv.mu.Unlock()
		//fmt.Printf("kvserver : Debug Get -- %v : %v : %v\n", kv.me, receivedOp.Key, kv.kvStorage[receivedOp.Key])
		return
	case <-time.After(10 * time.Second):
		if !kv.checkIsLeader() {
			reply.Err = ErrWrongLeader
			return
		} else {
			fmt.Printf("%v : Error -- Still waiting for get channel as leader\n", kv.me)
			reply.Err = ErrWrongLeader
		}
	}
	//fmt.Printf("kvserver : Debug Get* -- %v : %v : %v\n", kv.me, args.Key, kv.kvStorage[args.Key])
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// if !kv.checkIsLeader() {
	// 	reply.Err = ErrWrongLeader
	// 	return
	// }

	newOp := Op{Key: args.Key, Value: args.Value}

	if args.Op == "Put" {
		newOp.OpCommand = PutCommand
	} else {
		newOp.OpCommand = AppendCommand
	}
	//fmt.Printf("server %v 1\n", kv.me)
	kv.mu.Lock()
	//fmt.Printf("server %v 2\n", kv.me)
	_, _, isLeader := kv.rf.Start(newOp)
	kv.mu.Unlock()
	//fmt.Printf("server %v 3\n", kv.me)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	//fmt.Printf("server 4\n")
	select {
	case receivedOp := <-kv.putAppendChan:
		reply.Err = OK
		kv.mu.Lock()
		if val, ok := kv.kvStorage[receivedOp.Key]; ok {
			// Key exists, append the string value to the original value
			kv.kvStorage[receivedOp.Key] = val + "" + receivedOp.Value
		} else {
			// Key does not exist, create a new key-value pair
			kv.kvStorage[receivedOp.Key] = receivedOp.Value
		}
		kv.mu.Unlock()
		//fmt.Printf("kvserver : Debug PutAppend -- %v : %v : %v\n", kv.me, receivedOp.Key, kv.kvStorage[receivedOp.Key])
		return
	case <-time.After(10 * time.Second):
		fmt.Printf("kvserver %v : Error -- Still waiting for get channel as leader\n", kv.me)
	}
}

func (kv *KVServer) applyChanListener() {
	for !kv.killed() {
		applyCommand := <-kv.applyCh
		receivedOp, _ := applyCommand.Command.(Op)
		if receivedOp.OpCommand == GetCommand {
			if kv.checkIsLeader() {
				kv.getChan <- receivedOp
			}
		} else {
			kv.putAppendChan <- receivedOp
		}
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.kvStorage = make(map[string]string)
	kv.getChan = make(chan Op)
	kv.putAppendChan = make(chan Op)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.applyChanListener()

	return kv
}
