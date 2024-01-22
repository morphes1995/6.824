package shardkv

import (
	"6.824/src/shardmaster"
	"log"
)

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrWrongGroup = "ErrWrongGroup"
	WrongLeader   = "WrongLeader"
	ErrNotReady   = "ErrNotReady"

	Append = "Append"
	Put    = "Put"
	Get    = "Get"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key       string
	Value     string
	Op        string // "Put" or "Append"
	ClientId  int64
	RequestId int64

	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key       string
	ClientId  int64
	RequestId int64

	// You'll have to add definitions here.
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}

type Configuration struct {
	Config shardmaster.Config
	Data   [shardmaster.NShards]map[string]string
	Ack    map[int64]int64
}

type MoveShardArgs struct {
	Num      int
	ShardIds []int
}

type MoveShardReply struct {
	Err  Err
	Data [shardmaster.NShards]map[string]string
	Ack  map[int64]int64
}

func CopyMap(m map[string]string) (copied map[string]string) {
	copied = make(map[string]string)
	for k, v := range m {
		copied[k] = v
	}
	return
}

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf("[shardkv]--"+format, a...)
	}
	return
}

func PrintDetail(kv *ShardKV, entry Op) {
	term, isLeader := kv.rf.GetState()
	//if !isLeader {
	//	return
	//}
	switch entry.Command {
	case Get:
		DPrintf("%v,gid %v,leader %v term %v, finish appendLogEntry ,op %v , key %v , shardId %v, shard %v, conf %v %v",
			kv.me, kv.gid, isLeader, term, entry.Command, entry.Key, key2shard(entry.Key), kv.data[key2shard(entry.Key)], kv.config.Num, kv.config.Shards)
	case Append, Put:
		DPrintf("%v, gid %v,leader %v term %v, finish appendLogEntry ,op %v , key %v, value %v,shardId %v, shard %v,  conf %v %v",
			kv.me, kv.gid, isLeader, term, entry.Command, entry.Key, entry.Value, key2shard(entry.Key), kv.data[key2shard(entry.Key)], kv.config.Num, kv.config.Shards)
	}

}
