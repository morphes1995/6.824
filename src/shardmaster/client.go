package shardmaster

//
// Shardmaster clerk.
//

import (
	"6.824/src/labrpc"
	"sync"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	mu            sync.Mutex
	clientId      int64
	requestId     int
	currentLeader int
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
	// Your code here.
	ck.clientId = nrand()
	ck.requestId = 0
	ck.currentLeader = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.Num = num
	args.ClientId = ck.clientId
	ck.mu.Lock()
	args.RequestId = ck.requestId
	ck.requestId++
	ck.mu.Unlock()

	for {
		server := ck.servers[ck.currentLeader]
		reply := QueryReply{}
		ok := server.Call("ShardMaster.Query", args, &reply)
		if ok && !reply.WrongLeader {
			return reply.Config
		}
		ck.currentLeader = (ck.currentLeader + 1) % len(ck.servers) // choose another possible raft leader
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers
	args.ClientId = ck.clientId
	ck.mu.Lock()
	args.RequestId = ck.requestId
	ck.requestId++
	ck.mu.Unlock()

	for {
		server := ck.servers[ck.currentLeader]
		reply := JoinReply{}
		ok := server.Call("ShardMaster.Join", args, &reply)
		if ok && !reply.WrongLeader {
			return
		}
		ck.currentLeader = (ck.currentLeader + 1) % len(ck.servers) // choose another possible raft leader
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	// Your code here.
	args.GIDs = gids
	args.ClientId = ck.clientId
	ck.mu.Lock()
	args.RequestId = ck.requestId
	ck.requestId++
	ck.mu.Unlock()

	for {
		server := ck.servers[ck.currentLeader]
		reply := LeaveReply{}
		ok := server.Call("ShardMaster.Leave", args, &reply)
		if ok && !reply.WrongLeader {
			return
		}
		ck.currentLeader = (ck.currentLeader + 1) % len(ck.servers) // choose another possible raft leader
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	args.ClientId = ck.clientId
	ck.mu.Lock()
	args.RequestId = ck.requestId
	ck.requestId++
	ck.mu.Unlock()

	for {
		server := ck.servers[ck.currentLeader]
		reply := MoveReply{}
		ok := server.Call("ShardMaster.Move", args, &reply)
		if ok && !reply.WrongLeader {
			return
		}
		ck.currentLeader = (ck.currentLeader + 1) % len(ck.servers) // choose another possible raft leader
	}
}
