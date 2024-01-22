package shardkv

import (
	"6.824/src/shardmaster"
	"time"
)

type CleanupShardArgs struct {
	Num     int
	ShardId int
}

type CleanupShardReply struct {
	WrongLeader bool
	Err         Err
}

type CleanUpOp struct {
	ConfigNum int
	ShardId   int
}

func (kv *ShardKV) sendCleanupShard(gid int, lastConfig *shardmaster.Config, args *CleanupShardArgs) bool {
	for _, oldGroupServer := range lastConfig.Groups[gid] {
		srv := kv.make_end(oldGroupServer)
		reply := &CleanupShardReply{}
		ok := srv.Call("ShardKV.CleanupShard", args, reply)
		if ok {
			if reply.Err == OK {
				return true
			}
			if reply.Err == ErrNotReady {
				continue
			}
		}
	}
	DPrintf("%v stale shard %v now belong to %v, and may not delete from old replica group %v",
		kv.me, args.ShardId, kv.config.Shards[args.ShardId], gid)
	return false
}

func (kv *ShardKV) CleanupShard(args *CleanupShardArgs, reply *CleanupShardReply) {
	entry := CleanUpOp{
		ConfigNum: args.Num,
		ShardId:   args.ShardId,
	}

	kv.mu.Lock()
	if args.Num > kv.config.Num {
		reply.WrongLeader = false
		reply.Err = ErrNotReady
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	// write raft log
	index, _, isLeader := kv.rf.Start(entry)
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = WrongLeader
		return
	}

	kv.mu.Lock()
	ch, ok := kv.pendingCleanUpRequests[index]
	if !ok {
		ch = make(chan CleanUpOp, 1)
		kv.pendingCleanUpRequests[index] = ch
	}
	kv.mu.Unlock()

	// wait for raft to commit and apply this op
	reply.WrongLeader = true
	reply.Err = WrongLeader
	select {
	case entryResult := <-ch:
		if entryResult == entry {
			reply.WrongLeader = false
			reply.Err = OK
		}
	case <-time.After(300 * time.Millisecond):
		DPrintf("%v wait CleanupShard timeout ", kv.me)
	}

	kv.mu.Lock()
	delete(kv.pendingCleanUpRequests, index) // remove stale chan
	kv.mu.Unlock()

	return
}

func (kv *ShardKV) applyCleanup(entry CleanUpOp, commitIndex int) {
	if entry.ConfigNum <= kv.config.Num {
		oldConf := kv.mck.Query(entry.ConfigNum)
		if kv.gid == oldConf.Shards[entry.ShardId] && kv.gid != kv.config.Shards[entry.ShardId] {
			// this replica group no longer own entry.ShardID
			DPrintf("%s cleared shard %v data, original %v", kv.myInfo(), entry.ShardId, kv.data[entry.ShardId])
			kv.data[entry.ShardId] = make(map[string]string)
		}
	}

	// notify pending operation
	ch, ok := kv.pendingCleanUpRequests[commitIndex]
	if ok {
		ch <- entry
	}
}
