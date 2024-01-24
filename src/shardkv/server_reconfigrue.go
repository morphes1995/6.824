package shardkv

import (
	"6.824/src/labgob"
	"6.824/src/shardmaster"
	"fmt"
	"sync"
	"time"
)

type ConfigurationOp struct {
	// configuration change
	Config shardmaster.Config
	Data   [shardmaster.NShards]map[string]string
	Ack    map[int64]int64
	Err    Err
}

// Reconfigure Process re-configurations one at a time, in order.
func (kv *ShardKV) Reconfigure() {
	for {
		time.Sleep(80 * time.Millisecond)
		// only leader of a replica group can detect configuration change
		if _, isLeader := kv.rf.GetState(); !isLeader {
			continue
		}

		kv.mu.Lock()
		curConfig := kv.config
		kv.mu.Unlock()

		nextConfig := kv.mck.Query(curConfig.Num + 1)
		if nextConfig.Num == curConfig.Num+1 {
			op, ok := kv.getConfigurationOp(nextConfig)
			if !ok || !kv.appendConfigurationLogEntry(op) {
				// reconfiguration failed in this turn
				continue
			}
		}
	}
}

func (kv *ShardKV) getConfigurationOp(nextConfig shardmaster.Config) (ConfigurationOp, bool) {
	confOp := ConfigurationOp{}
	confOp.Config = nextConfig
	confOp.Ack = make(map[int64]int64)
	for i := 0; i < shardmaster.NShards; i++ {
		confOp.Data[i] = make(map[string]string)
	}

	groupToMoveShards := kv.findShardsShouldMove(nextConfig) // groups that contains shards to move

	ok := kv.fetchShardsInfoFromOldReplicaGroup(&confOp, groupToMoveShards, nextConfig)
	return confOp, ok
}

func (kv *ShardKV) fetchShardsInfoFromOldReplicaGroup(confOp *ConfigurationOp, groupToMoveShards map[int][]int, nextConfig shardmaster.Config) bool {
	retOk := true
	var ackMutex sync.Mutex
	var wg sync.WaitGroup
	for gid, shardsToMoveThere := range groupToMoveShards {
		if len(shardsToMoveThere) == 0 {
			continue
		}
		wg.Add(1)
		go func(confOp *ConfigurationOp, gid int, shardsToMoveThere []int) {
			defer wg.Done()

			moveArgs := MoveShardArgs{}
			moveArgs.Num = nextConfig.Num
			moveArgs.ShardIds = shardsToMoveThere
			reply := MoveShardReply{}
			//  send request to ask the corresponding server to move requested shards data to me
			if kv.sendMoveShard(gid, &moveArgs, &reply) {
				ackMutex.Lock()
				for shardIndex, shardData := range reply.Data {
					for k, v := range shardData {
						confOp.Data[shardIndex][k] = v
					}
				}
				for clientId := range reply.Ack {
					_, ok := confOp.Ack[clientId]
					if !ok || confOp.Ack[clientId] < reply.Ack[clientId] {
						confOp.Ack[clientId] = reply.Ack[clientId]
					}
				}
				ackMutex.Unlock()
			} else {
				retOk = false
			}
		}(confOp, gid, shardsToMoveThere)
	}

	// Make sure that all replica servers in a replica group do the migration at the same point
	// they all either accept or reject concurrent client requests.
	wg.Wait()

	return retOk
}

func (kv *ShardKV) findShardsShouldMove(nextConfig shardmaster.Config) map[int][]int {
	groupToMoveShards := make(map[int][]int)
	for i := 0; i < shardmaster.NShards; i++ {
		gid := kv.config.Shards[i]
		if gid == 0 {
			continue
		}
		// for each shard,  find the shards that not belong to this replica group ,
		// but will belong to this replica group in next confOp.  these shards will be moved to me latter
		if gid != kv.gid && nextConfig.Shards[i] == kv.gid {
			groupToMoveShards[gid] = append(groupToMoveShards[gid], i)
		}
	}

	return groupToMoveShards
}

func (kv *ShardKV) sendMoveShard(gid int, args *MoveShardArgs, reply *MoveShardReply) bool {
	// make sure at least one server in source replica group was applied new configuration
	for _, server := range kv.config.Groups[gid] {
		srv := kv.make_end(server)
		ok := srv.Call("ShardKV.MoveShard", args, reply)
		DPrintf("%s  moved shard from %v, gid %v, args.shardIds %v, reply data %v, ok %v, reply err %v, conf %v -> %v",
			kv.myInfo(), server, gid, args.ShardIds, reply.Data, ok, reply.Err, kv.config.Num, args.Num)
		if ok {
			if reply.Err == OK {
				return true
			} else {
				continue
			}
		}
	}
	return false
}

func (kv *ShardKV) myInfo() string {
	return fmt.Sprintf("%v gid %v ", kv.me, kv.gid)
}

func (kv *ShardKV) MoveShard(args *MoveShardArgs, reply *MoveShardReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = WrongLeader
		return
	}

	// this shard server may still handle the requested shards
	if kv.config.Num < args.Num {
		// todo may exists deadlock
		//  a pair of groups may need to move shards in both directions between them
		//  in case shard1 (group1 -> group2) while shard2 (group2->group1)
		reply.Err = ErrNotReady
		DPrintf("%s may exists deadlock kv.config.Num %v args.Num %v", kv.myInfo(), kv.config.Num, args.Num)
		return
	}

	for i := 0; i < shardmaster.NShards; i++ {
		reply.Data[i] = make(map[string]string)
	}
	for _, shardId := range args.ShardIds {
		reply.Data[shardId] = CopyMap(kv.data[shardId])
	}
	// the shards that replica group no longer owns will be removed when it sent to target replica group successfully
	reply.Ack = make(map[int64]int64)
	for clientId, requestId := range kv.ack { // todo may be we can only send acks about the moved shards
		reply.Ack[clientId] = requestId
	}
	reply.Err = OK
}

func (kv *ShardKV) appendConfigurationLogEntry(entry ConfigurationOp) bool {
	// write raft log
	index, _, isLeader := kv.rf.Start(entry)
	if !isLeader {
		return false
	}

	kv.mu.Lock()
	ch, ok := kv.pendingConfigurationRequests[index]
	if !ok {
		ch = make(chan ConfigurationOp, 1)
		kv.pendingConfigurationRequests[index] = ch
	}
	kv.mu.Unlock()

	// wait for raft to commit and apply this op
	result := false
	select {
	case entryResult := <-ch:
		if entryResult.Config.Num == entry.Config.Num && entryResult.Err == OK {
			result = true
		}
	case <-time.After(300 * time.Millisecond):
		DPrintf("%s wait appendConfigurationLogEntry timeout ", kv.myInfo())
	}

	kv.mu.Lock()
	delete(kv.pendingConfigurationRequests, index) // remove stale chan
	kv.mu.Unlock()

	DPrintf("%s finish appendConfigurationLogEntry ,conf %v,  result %v ", kv.myInfo(), entry.Config, result)
	return result

}

func (kv *ShardKV) applyConfigurationChange(conf ConfigurationOp, commitIndex int) {
	if conf.Config.Num <= kv.config.Num {
		return
	}

	err := OK
	oldConf := kv.config
	if conf.Config.Num != kv.config.Num+1 {
		err = ErrNotReady
	} else {
		for shardId, shardData := range conf.Data {
			for k, v := range shardData {
				kv.data[shardId][k] = v
			}
		}
		for clientId := range conf.Ack {
			_, ok := kv.ack[clientId]
			if !ok || kv.ack[clientId] < conf.Ack[clientId] {
				kv.ack[clientId] = conf.Ack[clientId]
			}
		}
		kv.config = conf.Config

	}

	conf.Err = Err(err)
	// notify pending operation
	ch, ok := kv.pendingConfigurationRequests[commitIndex]
	if ok {
		ch <- conf
	}

	if err == OK {
		for shardId, shardData := range conf.Data {
			if len(shardData) > 0 {
				oldGid := oldConf.Shards[shardId]
				args := CleanupShardArgs{
					ShardId: shardId,
					Num:     oldConf.Num,
				}
				go kv.sendCleanupShard(oldGid, &oldConf, &args)
			}
		}
	}

}

// InitReconfiguration init reconfiguration subsystem
func InitReconfiguration(kv *ShardKV) {
	labgob.Register(ConfigurationOp{})
	labgob.Register(shardmaster.Config{})
	labgob.Register(CleanUpOp{})

	kv.pendingConfigurationRequests = make(map[int]chan ConfigurationOp)
	kv.pendingCleanUpRequests = make(map[int]chan CleanUpOp)
	go kv.Reconfigure()

}
