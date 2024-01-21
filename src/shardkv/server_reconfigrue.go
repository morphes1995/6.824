package shardkv

import (
	"6.824/src/labgob"
	"6.824/src/shardmaster"
	"sync"
	"time"
)

type ConfigurationOp struct {
	// configuration change
	Config shardmaster.Config
	Data   [shardmaster.NShards]map[string]string
	Ack    map[int64]int64
}

func (kv *ShardKV) Reconfigure() {
	for {
		// only leader of a replica group can detect configuration change
		if _, isLeader := kv.rf.GetState(); !isLeader {
			continue
		}

		latestConf := kv.mck.Query(-1)
		// for each newer configuration changes
		for i := kv.config.Num + 1; i <= latestConf.Num; i++ {
			conf := kv.mck.Query(i)
			args, ok := kv.getConfiguration(conf)

			if !ok || !kv.updateConfiguration(args) {
				// reconfiguration failed in this turn
				break
			}
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) getConfiguration(nextConfig shardmaster.Config) (Configuration, bool) {
	configuration := Configuration{}
	configuration.Config = nextConfig
	configuration.Ack = make(map[int64]int64)
	for i := 0; i < shardmaster.NShards; i++ {
		configuration.Data[i] = make(map[string]string)
	}

	groupToMoveShards := make(map[int][]int) // groups that contains shards to move
	for i := 0; i < shardmaster.NShards; i++ {
		// for each shard,  find the shards that not belong to this replica group ,
		// but will belong to this replica group in next configuration.  and then move them to me
		if kv.config.Shards[i] != kv.gid && nextConfig.Shards[i] == kv.gid {
			gid := kv.config.Shards[i]
			if gid != 0 {
				if _, ok := groupToMoveShards[gid]; !ok {
					groupToMoveShards[gid] = []int{i}
				} else {
					groupToMoveShards[gid] = append(groupToMoveShards[gid], i)
				}
			}
		}
	}

	retOk := true
	var ackMutex sync.Mutex
	var wait sync.WaitGroup
	for gid, shardsToMoveThere := range groupToMoveShards {
		if len(shardsToMoveThere) == 0 {
			continue
		}
		wait.Add(1)
		go func(gid int, shardsToMoveThere []int) {
			defer wait.Done()

			moveArgs := MoveShardArgs{}
			moveArgs.Num = nextConfig.Num
			moveArgs.ShardIds = shardsToMoveThere
			reply := MoveShardReply{}
			//  send request to ask the corresponding server to move requested shards data to me
			if kv.sendMoveShard(gid, &moveArgs, &reply) {
				ackMutex.Lock()
				for shardIndex, shardData := range reply.Data {
					for k, v := range shardData {
						configuration.Data[shardIndex][k] = v
					}
				}
				for clientId := range reply.Ack {
					_, ok := configuration.Ack[clientId]
					if !ok || configuration.Ack[clientId] < reply.Ack[clientId] {
						configuration.Ack[clientId] = reply.Ack[clientId]
					}
				}
				ackMutex.Unlock()
			} else {
				retOk = false
			}
		}(gid, shardsToMoveThere)
	}
	wait.Wait()
	return configuration, retOk
}

func (kv *ShardKV) sendMoveShard(gid int, args *MoveShardArgs, reply *MoveShardReply) bool {
	// Make sure that all replica servers in a replica group do the migration at the same point
	// they all either accept or reject concurrent client requests.
	for _, server := range kv.config.Groups[gid] {
		srv := kv.make_end(server)
		ok := srv.Call("ShardKV.MoveShard", args, reply)
		DPrintf("%v  moved shard from %v, gid %v, args.shardIds %v, reply data %v, ok %v, reply err %v", kv.me, server, gid, args.ShardIds, reply.Data, ok, reply.Err)
		if ok {
			if reply.Err == OK {
				return true
			}
			if reply.Err == ErrNotReady {
				continue
			}
		}
	}
	// all source  replica server are not ready,wait for next loop
	return false
}

func (kv *ShardKV) MoveShard(args *MoveShardArgs, reply *MoveShardReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// this shard server may still handle the requested shards
	if kv.config.Num < args.Num {
		// todo may exists deadlock
		//  a pair of groups may need to move shards in both directions between them
		//  in case shard1 (group1 -> group2) while shard2 (group2->group1)
		reply.Err = ErrNotReady
		return
	}

	for i := 0; i < shardmaster.NShards; i++ {
		reply.Data[i] = make(map[string]string)
	}
	for _, shardId := range args.ShardIds {
		reply.Data[shardId] = CopyMap(kv.data[shardId])
	}
	// todo it is acceptable to continue to store shards that it no longer owns

	reply.Ack = make(map[int64]int64)
	for clientId, requestId := range kv.ack { // todo may be we can only send acks about the moved shards
		reply.Ack[clientId] = requestId
	}
	reply.Err = OK
}

func (kv *ShardKV) updateConfiguration(conf Configuration) bool {
	entry := ConfigurationOp{
		Config: conf.Config,
		Data:   conf.Data,
		Ack:    conf.Ack,
	}

	result := kv.appendConfigurationLogEntry(entry)
	return result
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
		if entryResult.Config.Num == entry.Config.Num {
			result = true
		}
	case <-time.After(300 * time.Millisecond):
		DPrintf("%v wait appendConfigurationLogEntry timeout ", kv.me)
	}

	kv.mu.Lock()
	delete(kv.pendingConfigurationRequests, index) // remove stale chan
	kv.mu.Unlock()

	DPrintf("%v finish appendConfigurationLogEntry ,conf %v,  result %v ", kv.me, entry.Config, result)
	return result

}

func (kv *ShardKV) applyConfigurationChange(conf ConfigurationOp, commitIndex int) {
	if conf.Config.Num <= kv.config.Num {
		return
	}

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

	// notify pending operation
	ch, ok := kv.pendingConfigurationRequests[commitIndex]
	if ok {
		ch <- conf
	}
}

// InitReconfiguration init reconfiguration subsystem
func InitReconfiguration(kv *ShardKV) {
	labgob.Register(ConfigurationOp{})
	labgob.Register(shardmaster.Config{})

	kv.pendingConfigurationRequests = make(map[int]chan ConfigurationOp)
	go kv.Reconfigure()

}
