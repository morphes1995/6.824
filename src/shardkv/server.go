package shardkv

import (
	"6.824/src/labgob"
	"6.824/src/labrpc"
	"6.824/src/raft"
	"6.824/src/shardmaster"
	"bytes"
	"sync"
	"sync/atomic"
	"time"
)

const (
	RespondTimeout          = 200
	ConfigureMonitorTimeout = 70
)

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int // replica group this kv server belong to
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	dead          int32
	stateMachines map[int]*Shard // each shard data and state
	//ack                          map[int64]int64
	notifyChans map[int]chan CommandResponse // notify client goroutine by applier goroutine to response

	lastConfig    shardmaster.Config
	currentConfig shardmaster.Config

	mck *shardmaster.Clerk

	lastOperations map[int64]OperationContext
	lastApplied    int
}

// check whether this raft group can serve this shard at present
// check before write raft log and before apply to SM
func (kv *ShardKV) canServe(shardID int) bool {
	return kv.currentConfig.Shards[shardID] == kv.gid &&
		(kv.stateMachines[shardID].Status == Serving || kv.stateMachines[shardID].Status == GCing)
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := &Op{
		Command:   Get,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		Key:       args.Key,
	}

	// pre-check invalid operations
	kv.mu.Lock()
	// return ErrWrongGroup directly to let client fetch latest configuration and perform a retry if this key can't be served by this shard at present
	if !kv.canServe(key2shard(args.Key)) {
		reply.WrongLeader = false
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	err, value := kv.appendLogEntry(NewOperationCommand(op))
	reply.WrongLeader = err == WrongLeader
	reply.Err = err
	reply.Value = value
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// pre-check invalid operations
	kv.mu.Lock()
	// return ErrWrongGroup directly to let client fetch latest configuration and perform a retry if this key can't be served by this shard at present
	if !kv.canServe(key2shard(args.Key)) {
		reply.WrongLeader = false
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}

	if kv.isDuplicateRequest(args.ClientId, args.RequestId) {
		DPrintf("request %d already processed", args.RequestId)
		lastOperationCtx := kv.lastOperations[args.ClientId]
		reply.Err = lastOperationCtx.LastResponse.Err
		reply.WrongLeader = reply.Err == WrongLeader
		kv.mu.Unlock()
		return
	}

	kv.mu.Unlock()

	op := &Op{
		Command:   args.Op,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		Key:       args.Key,
		Value:     args.Value,
	}
	err, _ := kv.appendLogEntry(NewOperationCommand(op))
	reply.WrongLeader = err == WrongLeader
	reply.Err = err
}

func (kv *ShardKV) appendLogEntry(command Command) (err Err, value string) {
	// write raft log
	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		return WrongLeader, ""
	}

	kv.mu.Lock()
	ch := kv.getNotifyChan(index)
	kv.mu.Unlock()

	select {
	case res := <-ch:
		err, value = res.Err, res.Value
	case <-time.After(RespondTimeout * time.Millisecond):
		err = WrongLeader
		DPrintf("%v-%v read from chan timeout command %v ", kv.gid, kv.me, command)
	}

	go kv.closeNotifyCh(index)

	return
}

func (kv *ShardKV) applier() {
	for kv.killed() == false {
		select {
		case message := <-kv.applyCh:
			//DPrintf("{Node %v}{Group %v} tries to apply message %v", kv.me, kv.gid, message)
			if message.IsSnapshot {
				kv.mu.Lock()
				kv.restoreSnapshot(message.Snapshot)
				kv.lastApplied = message.LastIncludedIndex
				kv.mu.Unlock()
			} else {
				kv.mu.Lock()
				if message.CommandIndex <= kv.lastApplied {
					DPrintf("{Node %v}{Group %v} discards outdated message %v because a newer snapshot which lastApplied is %v has been restored", kv.me, kv.gid, message, kv.lastApplied)
					kv.mu.Unlock()
					continue
				}
				kv.lastApplied = message.CommandIndex

				var response CommandResponse
				command := message.Command.(Command)
				switch command.Op {
				case Operation:
					operation := command.Data.(Op)
					response = kv.applyOperation(&message, &operation)
				case Configuration:
					nextConfig := command.Data.(shardmaster.Config)
					response = kv.applyConfiguration(&nextConfig)
				case InsertShards:
					shardsInfo := command.Data.(ShardOperationResponse)
					response = kv.applyInsertShards(&shardsInfo)
				case DeleteShards:
					shardsInfo := command.Data.(ShardOperationRequest)
					response = kv.applyDeleteShards(&shardsInfo)
				case EmptyEntry:
					response = kv.applyEmptyEntry()
				}

				// only notify related channel for currentTerm's log when node is leader
				if currentTerm, isLeader := kv.rf.GetState(); isLeader && message.CommandTerm == currentTerm {
					ch := kv.getNotifyChan(message.CommandIndex)
					ch <- response
				}

				needSnapshot := kv.needSnapshot()
				if needSnapshot {
					kv.takeSnapshot(message.CommandIndex)
				}
				kv.mu.Unlock()
			}
		}
	}
}

func (kv *ShardKV) needSnapshot() bool {
	return kv.maxraftstate != -1 && kv.rf.GetStateSize() >= kv.maxraftstate
}

func (kv *ShardKV) takeSnapshot(commandIndex int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(kv.stateMachines)
	e.Encode(kv.lastOperations)
	e.Encode(kv.currentConfig)
	e.Encode(kv.lastConfig)

	go kv.rf.CreateSnapShot(w.Bytes(), commandIndex)
}

func (kv *ShardKV) restoreSnapshot(snapshot []byte) {
	var lastIncludedIndex, lastIncludedTerm int
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	d.Decode(&lastIncludedIndex)
	d.Decode(&lastIncludedTerm)
	d.Decode(&kv.stateMachines)
	d.Decode(&kv.lastOperations)
	d.Decode(&kv.currentConfig)
	d.Decode(&kv.lastConfig)
}

// Kill the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) applyOperation(msg *raft.ApplyMsg, op *Op) CommandResponse {
	var response CommandResponse
	shardID := key2shard(op.Key)
	if kv.canServe(shardID) {
		if op.Command != Get && kv.isDuplicateRequest(op.ClientId, op.RequestId) {
			DPrintf("{Node %v}{Group %v} doesn't apply duplicated message %v to stateMachine because max request id is %v for client %v", kv.me, kv.gid, msg, kv.lastOperations[op.ClientId], op.ClientId)
			return kv.lastOperations[op.ClientId].LastResponse
		} else {
			response = kv.applyLogToStateMachines(op, shardID)
			if op.Command != Get {
				kv.lastOperations[op.ClientId] = OperationContext{op.RequestId, response}
			}
			return response
		}
	}
	return CommandResponse{ErrWrongGroup, ""}

}

func (kv *ShardKV) isDuplicateRequest(clientId, requestId int64) bool {
	lastOperationCtx, ok := kv.lastOperations[clientId]
	if ok {
		return lastOperationCtx.LastRequestId >= requestId
	}
	return false
}

func (kv *ShardKV) applyLogToStateMachines(op *Op, shardId int) CommandResponse {
	resp := CommandResponse{}
	switch op.Command {
	case Get:
		resp.Value, resp.Err = kv.stateMachines[shardId].Get(op.Key)
	case Put:
		resp.Err = kv.stateMachines[shardId].Put(op.Key, op.Value)
	case Append:
		resp.Err = kv.stateMachines[shardId].Append(op.Key, op.Value)
	}
	_, isleader := kv.rf.GetState()
	DPrintf("%v-%v,is leader:%v, clientId-reqId %v-%v op %v apply to kv.data,   key %v, value  %v , shardId %v, value in kv.data %v , resp %v",
		kv.gid, kv.me, isleader, op.ClientId, op.RequestId, op.Command, op.Key, op.Value, key2shard(op.Key), kv.stateMachines[key2shard(op.Key)], resp)

	return resp
}

func (kv *ShardKV) getNotifyChan(commandIndex int) chan CommandResponse {
	if _, existCh := kv.notifyChans[commandIndex]; !existCh {
		kv.notifyChans[commandIndex] = make(chan CommandResponse, 1)
	}
	return kv.notifyChans[commandIndex]
}

func (kv *ShardKV) closeNotifyCh(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if _, ok := kv.notifyChans[index]; ok {
		close(kv.notifyChans[index])
		delete(kv.notifyChans, index)
	}
}

// StartServer servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(CommandResponse{})
	labgob.Register(Command{})
	//labgob.Register(CommandRequest{})
	labgob.Register(shardmaster.Config{})
	labgob.Register(ShardOperationResponse{})
	labgob.Register(ShardOperationRequest{})

	applyCh := make(chan raft.ApplyMsg, 2000)
	kv := &ShardKV{
		me:             me,
		dead:           0,
		rf:             raft.Make(servers, me, persister, applyCh),
		applyCh:        applyCh,
		make_end:       make_end,
		gid:            gid,
		masters:        masters,
		mck:            shardmaster.MakeClerk(masters),
		lastApplied:    0,
		maxraftstate:   maxraftstate,
		stateMachines:  make(map[int]*Shard),
		currentConfig:  shardmaster.Config{},
		lastConfig:     shardmaster.Config{},
		lastOperations: make(map[int64]OperationContext),
		notifyChans:    make(map[int]chan CommandResponse),
	}

	for i := 0; i < shardmaster.NShards; i++ {
		kv.stateMachines[i] = NewShard()
	}

	//kv.restoreSnapshot(persister.ReadSnapshot())

	// Use something like this to talk to the shardmaster:

	// start applier goroutine to apply committed logs to stateMachine
	go kv.applier()
	// start configuration monitor goroutine to fetch latest configuration
	go kv.Monitor(kv.configureAction, ConfigureMonitorTimeout*time.Millisecond)
	// start migration monitor goroutine to pull related shards
	go kv.Monitor(kv.migrationAction, ConfigureMonitorTimeout*time.Millisecond)
	// start gc monitor goroutine to delete useless shards in remote groups
	go kv.Monitor(kv.gcAction, ConfigureMonitorTimeout*time.Millisecond)

	// start entry-in-currentTerm monitor goroutine to advance commitIndex by appending empty entries in current term periodically to avoid live locks
	go kv.Monitor(kv.checkEntryInCurrentTermAction, 150*time.Millisecond)

	return kv
}

func (kv *ShardKV) Monitor(action func(), timeout time.Duration) {
	for kv.killed() == false {
		if _, isLeader := kv.rf.GetState(); isLeader {
			action()
		}
		time.Sleep(timeout)
	}
}

func (kv *ShardKV) configureAction() {
	canPerformNextConfig := true
	kv.mu.Lock()
	for _, shard := range kv.stateMachines {
		if shard.Status != Serving {
			canPerformNextConfig = false
			//DPrintf("{Node %v}{Group %v} will not try to fetch latest configuration because shard %v's status is %v when currentConfig is %v",kv.me, kv.gid, shardId, shard.Status, kv.currentConfig)
			break
		}
	}
	currentConfigNum := kv.currentConfig.Num
	kv.mu.Unlock()

	if canPerformNextConfig {
		nextConfig := kv.mck.Query(currentConfigNum + 1)
		if nextConfig.Num == currentConfigNum+1 {
			//DPrintf("{Node %v}{Group %v} fetches latest configuration %v when currentConfigNum is %v",
			//	kv.me, kv.gid, nextConfig, currentConfigNum)
			kv.appendLogEntry(NewConfigurationCommand(&nextConfig))
		}
	}
}

func (kv *ShardKV) applyConfiguration(nextConfig *shardmaster.Config) CommandResponse {
	DPrintf("{Node %v}{Group %v} updates currentConfig from %v to %v", kv.me, kv.gid, kv.currentConfig, nextConfig)
	if nextConfig.Num == kv.currentConfig.Num+1 {
		kv.updateShardStatus(nextConfig)
		kv.lastConfig = kv.currentConfig
		kv.currentConfig = *nextConfig
		return CommandResponse{OK, ""}
	}
	DPrintf("{Node %v}{Group %v} not ready, rejects config %v when currentConfig is %v",
		kv.me, kv.gid, nextConfig, kv.currentConfig)
	return CommandResponse{ErrNotReady, ""}
}

func (kv *ShardKV) updateShardStatus(nextConfig *shardmaster.Config) {
	for shardId, gid := range nextConfig.Shards {
		// 如果之前没有该shard，新配置里又有了，则将该shard状态设为等待从前任所有者迁移shard过来
		if kv.currentConfig.Shards[shardId] != kv.gid && gid == kv.gid {
			if nextConfig.Num == 1 { // 代表刚初始化集群，不需要去其他group获取shard，直接设为Exist
				kv.stateMachines[shardId].Status = Serving
			} else {
				kv.stateMachines[shardId].Status = Pulling
			}
		}

		// 如果曾经拥有该shard但新配置里没有了，则将该shard状态设为等待迁移走
		if kv.currentConfig.Shards[shardId] == kv.gid && gid != kv.gid {
			kv.stateMachines[shardId].Status = BePulling
		}
		// 之前有现在也有，之前没有现在也没有的情况均不需要迁移shard
	}
}

func (kv *ShardKV) migrationAction() {
	kv.mu.Lock()
	gid2shardIDs := kv.getShardIDsByStatus(Pulling)
	var wg sync.WaitGroup
	for gid, shardIDs := range gid2shardIDs {
		//DPrintf("{Node %v}{Group %v} starts a PullTask to get shards %v from group %v when config is %v", kv.me, kv.gid, shardIDs, gid, kv.currentConfig)
		wg.Add(1)
		go func(servers []string, configNum int, shardIDs []int) {
			defer wg.Done()
			pullTaskRequest := ShardOperationRequest{configNum, shardIDs}
			for _, server := range servers {
				var pullTaskResponse ShardOperationResponse
				srv := kv.make_end(server)
				if srv.Call("ShardKV.GetShardsData", &pullTaskRequest, &pullTaskResponse) && pullTaskResponse.Err == OK {
					DPrintf("{Node %v}{Group %v} gets a PullTaskResponse %v and tries to commit it when currentConfigNum is %v",
						kv.me, kv.gid, pullTaskResponse, configNum)

					kv.appendLogEntry(NewInsertShardsCommand(&pullTaskResponse))
				}
			}
		}(kv.lastConfig.Groups[gid], kv.currentConfig.Num, shardIDs)
	}

	kv.mu.Unlock()
	wg.Wait()
}

func (kv *ShardKV) GetShardsData(request *ShardOperationRequest, response *ShardOperationResponse) {
	// only pull shards from leader
	if _, isLeader := kv.rf.GetState(); !isLeader {
		response.Err = WrongLeader
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	defer DPrintf("{Node %v}{Group %v} processes PullTaskRequest %v with response %v", kv.me, kv.gid, request, response)

	if kv.currentConfig.Num < request.ConfigNum {
		response.Err = ErrNotReady
		return
	}

	response.Shards = make(map[int]map[string]string)
	for _, shardID := range request.ShardIDs {
		response.Shards[shardID] = kv.stateMachines[shardID].deepCopy()
	}

	response.LastOperations = make(map[int64]OperationContext)
	for clientID, operation := range kv.lastOperations {
		response.LastOperations[clientID] = operation.deepCopy()
	}

	response.ConfigNum, response.Err = request.ConfigNum, OK
}

func (kv *ShardKV) applyInsertShards(shardsInfo *ShardOperationResponse) CommandResponse {
	if shardsInfo.ConfigNum == kv.currentConfig.Num {
		for shardId, shardData := range shardsInfo.Shards {
			shard := kv.stateMachines[shardId]
			if shard.Status == Pulling {
				for key, value := range shardData {
					shard.KV[key] = value
				}
				shard.Status = GCing
			} else {
				DPrintf("{Node %v}{Group %v} encounters duplicated shards insertion %v when currentConfig is %v",
					kv.me, kv.gid, shardsInfo, kv.currentConfig)
				break
			}
		}
		for clientId, operationContext := range shardsInfo.LastOperations {
			if lastOperation, ok := kv.lastOperations[clientId]; !ok || lastOperation.LastRequestId < operationContext.LastRequestId {
				kv.lastOperations[clientId] = operationContext
			}
		}
		DPrintf("{Node %v}{Group %v} accepts shards insertion %v when currentConfig is %v", kv.me, kv.gid, shardsInfo, kv.currentConfig)
		return CommandResponse{OK, ""}
	}
	DPrintf("{Node %v}{Group %v} rejects outdated shards insertion %v when currentConfig is %v", kv.me, kv.gid, shardsInfo, kv.currentConfig)
	return CommandResponse{ErrNotReady, ""}
}

func (kv *ShardKV) getShardIDsByStatus(status ShardStatus) (gid2shardIDs map[int][]int) {
	gid2shardIDs = make(map[int][]int)
	for shardId, shard := range kv.stateMachines {
		if shard.Status == status {
			gid := kv.lastConfig.Shards[shardId]
			gid2shardIDs[gid] = append(gid2shardIDs[gid], shardId)
		}
	}
	return
}

func (kv *ShardKV) gcAction() {
	kv.mu.Lock()
	gid2shardIDs := kv.getShardIDsByStatus(GCing)
	var wg sync.WaitGroup
	for gid, shardIDs := range gid2shardIDs {
		DPrintf("{Node %v}{Group %v} starts a GCTask to delete shards %v in group %v when config is %v", kv.me, kv.gid, shardIDs, gid, kv.currentConfig)
		wg.Add(1)
		go func(servers []string, configNum int, shardIDs []int) {
			defer wg.Done()
			gcTaskRequest := ShardOperationRequest{configNum, shardIDs}
			for _, server := range servers {
				var gcTaskResponse ShardOperationResponse
				srv := kv.make_end(server)
				if srv.Call("ShardKV.DeleteShardsData", &gcTaskRequest, &gcTaskResponse) && gcTaskResponse.Err == OK {
					DPrintf("{Node %v}{Group %v} deletes shards %v in remote group successfully when currentConfigNum is %v", kv.me, kv.gid, shardIDs, configNum)
					kv.appendLogEntry(NewDeleteShardsCommand(&gcTaskRequest)) // update shard status from GCing to Serving
				}
			}
		}(kv.lastConfig.Groups[gid], kv.currentConfig.Num, shardIDs)
	}
	kv.mu.Unlock()
	wg.Wait()
}

func (kv *ShardKV) DeleteShardsData(request *ShardOperationRequest, response *ShardOperationResponse) {
	// only delete shards when role is leader
	if _, isLeader := kv.rf.GetState(); !isLeader {
		response.Err = WrongLeader
		return
	}

	defer DPrintf("{Node %v}{Group %v} processes GCTaskRequest %v with response %v", kv.me, kv.gid, request, response)

	kv.mu.Lock()
	if kv.currentConfig.Num > request.ConfigNum {
		DPrintf("{Node %v}{Group %v}'s encounters duplicated shards deletion %v when currentConfig is %v", kv.me, kv.gid, request, kv.currentConfig)
		response.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	err, _ := kv.appendLogEntry(NewDeleteShardsCommand(request)) // update shard status from BePulling to Serving
	response.Err = err
}

func (kv *ShardKV) applyDeleteShards(shardsInfo *ShardOperationRequest) CommandResponse {
	if shardsInfo.ConfigNum == kv.currentConfig.Num {
		DPrintf("{Node %v}{Group %v}'s shards status are %v before accepting shards deletion %v when currentConfig is %v",
			kv.me, kv.gid, kv.stateMachines, shardsInfo, kv.currentConfig)
		for _, shardId := range shardsInfo.ShardIDs {
			shard := kv.stateMachines[shardId]
			if shard.Status == GCing {
				shard.Status = Serving
			} else if shard.Status == BePulling {
				kv.stateMachines[shardId] = NewShard()
			} else {
				DPrintf("{Node %v}{Group %v} encounters duplicated shards deletion %v when currentConfig is %v", kv.me, kv.gid, shardsInfo, kv.currentConfig)
				break
			}
		}
		DPrintf("{Node %v}{Group %v}'s shards status are %v after accepting shards deletion %v when currentConfig is %v",
			kv.me, kv.gid, kv.stateMachines, shardsInfo, kv.currentConfig)
		return CommandResponse{OK, ""}
	}
	// 仅可执行与当前配置版本相同地分片删除日志，否则已经删除过，直接返回 OK 即可。
	DPrintf("{Node %v}{Group %v}'s encounters duplicated shards deletion %v when currentConfig is %v", kv.me, kv.gid, shardsInfo, kv.currentConfig)
	return CommandResponse{OK, ""}
}

func (kv *ShardKV) checkEntryInCurrentTermAction() {
	if !kv.rf.HasLogInCurrentTerm() {
		// write raft log
		kv.rf.Start(NewEmptyEntryCommand())
	}
}

func (kv *ShardKV) applyEmptyEntry() CommandResponse {
	return CommandResponse{OK, ""}
}
