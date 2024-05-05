package kvraft

import (
	"crypto/rand"
	"math/big"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderId int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.leaderId = 0
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	args := GetArgs{Key: key}
	reply := GetReply{}
	//fmt.Printf("kvclerk : Debug -- Get %v\n", key)
	server := 0
	for {
		ok := ck.servers[server].Call("KVServer.Get", &args, &reply)
		//fmt.Printf("kvclerk: get\n")
		if !ok || reply.Err == ErrWrongLeader {
			server = (server + 1) % len(ck.servers)
			continue
		} else if reply.Err == ErrNoKey {
			return ""
		}
		// if ok && reply.Err == OK {
		// 	ck.leaderId = server
		// 	return reply.Value
		// }
		//fmt.Printf("kvclerk : Debug -- Get %v : %v\n", key, reply.Value)
		return reply.Value
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{Key: key, Value: value, Op: op}
	reply := PutAppendReply{}
	//fmt.Printf("kvclerk : Debug -- %v : %v : %v\n", op, key, value)
	server := 0
	for {
		// fmt.Printf("kvclerk: putAppend\n")
		ok := ck.servers[server].Call("KVServer.PutAppend", &args, &reply)
		if ok && reply.Err == OK {
			//ck.leaderId = server
			return
		} else if reply.Err == ErrWrongLeader {
			server = (server + 1) % len(ck.servers)
			continue
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
