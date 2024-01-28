package shardkv

import (
	"6.824/src/shardmaster"
	"fmt"
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

//type Configuration struct {
//	Config shardmaster.Config
//	Data   [shardmaster.NShards]map[string]string
//	Ack    map[int64]int64
//}

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

func PrintDetail(kv *ShardKV, entry Op, err Err) {
	DPrintf("%v-%v, finish appendLogEntry ,op %v , key %v, err %v , shardId %v, shard %v, conf %v %v",
		kv.gid, kv.me, entry.Command, entry.Key, err, key2shard(entry.Key), kv.stateMachines[key2shard(entry.Key)], kv.currentConfig.Num, kv.currentConfig.Shards)

}

type CommandType uint8

const (
	Operation CommandType = iota
	Configuration
	InsertShards
	DeleteShards
	EmptyEntry
)

type Command struct {
	Op   CommandType
	Data interface{}
}

func (command Command) String() string {
	return fmt.Sprintf("{Type:%v,Data:%v}", command.Op, command.Data)
}

func NewOperationCommand(op *Op) Command {
	return Command{Operation, *op}
}

func NewConfigurationCommand(config *shardmaster.Config) Command {
	return Command{Configuration, *config}
}

func NewInsertShardsCommand(response *ShardOperationResponse) Command {
	return Command{InsertShards, *response}
}

func NewDeleteShardsCommand(request *ShardOperationRequest) Command {
	return Command{DeleteShards, *request}
}

func NewEmptyEntryCommand() Command {
	return Command{EmptyEntry, nil}
}

//type CommandRequest struct {
//	//Servers   map[int][]string // for Join
//	//GIDs      []int            // for Leave
//	//Shard     int              // for Move
//	//GID       int              // for Move
//	//Num       int              // for Query
//	Op        Op
//	ClientId  int64
//	CommandId int64
//}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	ClientId  int64
	RequestId int64
	Command   string

	Key   string
	Value string
}

type CommandResponse struct {
	Err   Err
	Value string
}
type ShardOperationRequest struct {
	ConfigNum int
	ShardIDs  []int
}
type ShardOperationResponse struct {
	Err            Err
	Shards         map[int]map[string]string
	ConfigNum      int
	LastOperations map[int64]OperationContext
}
