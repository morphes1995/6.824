package shardmaster

import (
	"6.824/src/labgob"
	"6.824/src/labrpc"
	"6.824/src/raft"
	"sort"
	"sync"
	"time"
)

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	ack             map[int64]int
	pendingRequests map[int]chan Op
	stopCh          chan any

	configs []Config // indexed by config num
}

const (
	JOIN  = "JOIN"
	LEAVE = "LEAVE"
	MOVE  = "MOVE"
	QUERY = "QUERY"
)

type Op struct {
	// Your data here.
	ClientId  int64
	RequestId int
	Command   string

	Servers map[int][]string /* JoinArgs */
	GIDs    []int            /*  LeaveArgs */
	Shard   int              /* MoveArgs */
	GID     int              /* MoveArgs */
	Num     int              /* QueryArgs */
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	entry := Op{
		Command:   JOIN,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		Servers:   args.Servers,
	}

	DPrintf("%d receive join req, args.servers %v", sm.me, args.Servers)
	ok := sm.appendLogEntry(entry)
	if !ok {
		reply.WrongLeader = true
		return
	}

	reply.WrongLeader = false
	reply.Err = OK
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	entry := Op{
		Command:   LEAVE,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		GIDs:      args.GIDs,
	}

	DPrintf("%d receive leave req, args.gids %v", sm.me, args.GIDs)
	ok := sm.appendLogEntry(entry)
	if !ok {
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false
	reply.Err = OK
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	entry := Op{
		Command:   MOVE,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		GID:       args.GID,
		Shard:     args.Shard,
	}

	DPrintf("%d receive move req, args.shard %d, args.gid %d", sm.me, args.Shard, args.GID)
	ok := sm.appendLogEntry(entry)
	if !ok {
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false
	reply.Err = OK
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	entry := Op{
		Command:   QUERY,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		Num:       args.Num,
	}

	ok := sm.appendLogEntry(entry)
	if !ok {
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false
	reply.Err = OK

	sm.mu.Lock()
	if args.Num == -1 || args.Num >= len(sm.configs) {
		reply.Config = sm.configs[len(sm.configs)-1]
	} else {
		reply.Config = sm.configs[args.Num]
	}
	sm.mu.Unlock()

	DPrintf("%d finish query req, args.num %v, reply.conf %v", sm.me, args.Num, reply.Config)
	reply.Err = OK
}

func (sm *ShardMaster) appendLogEntry(entry Op) bool {
	sm.mu.Lock()
	requestId, ok := sm.ack[entry.ClientId]
	if ok && requestId >= entry.RequestId { // ignore requests that already processed
		sm.mu.Unlock()
		return true
	}
	sm.mu.Unlock()

	index, _, isLeader := sm.rf.Start(entry)
	if !isLeader {
		return false
	}

	sm.mu.Lock()
	ch, ok := sm.pendingRequests[index]
	if !ok {
		ch = make(chan Op, 1)
		sm.pendingRequests[index] = ch
	}
	sm.mu.Unlock()

	// wait for raft to commit and apply this op
	result := false
	select {
	case entryApplied := <-ch:
		// logs[index] may be changed and is not equal to `entry` , because the leadership change
		// and old leader's log entry was overwritten by new leader's
		result = sm.isMatch(entry, entryApplied)
	case <-time.After(300 * time.Millisecond):
	}

	sm.mu.Lock()
	delete(sm.pendingRequests, index) // remove stale chan
	sm.mu.Unlock()

	return result
}

// check if the result corresponds to the log entry.
func (sm *ShardMaster) isMatch(entry1 Op, entry2 Op) bool {
	return entry1.ClientId == entry2.ClientId && entry1.RequestId == entry2.RequestId
}

func (sm *ShardMaster) Run() {
	for {
		select {
		// receive applied log entry from raft
		case msg := <-sm.applyCh:
			sm.mu.Lock()
			entry := msg.Command.(Op)
			requestId, ok := sm.ack[entry.ClientId]
			if !ok || requestId < entry.RequestId {
				// apply state to sm server
				sm.applyEntry(entry)
				sm.ack[entry.ClientId] = entry.RequestId
			}

			// notify pending operation
			ch, ok := sm.pendingRequests[msg.CommandIndex]
			if ok {
				ch <- entry
			}
			// configuration log will not be large, no need to create snapshot
			sm.mu.Unlock()

		case <-sm.stopCh:
			return

		}
	}
}

func (sm *ShardMaster) applyEntry(entry Op) {
	switch entry.Command {
	case JOIN:
		sm.applyJoin(entry)
	case LEAVE:
		sm.applyLeave(entry)
	case MOVE:
		sm.applyMove(entry)
	case QUERY:
		// do nothing
	}
}

// Kill : the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
	sm.stopCh <- true
}

// Raft needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

func (sm *ShardMaster) newConfig() *Config {
	conf := &Config{}
	latestConf := sm.configs[len(sm.configs)-1]
	conf.Num = latestConf.Num + 1
	conf.Shards = latestConf.Shards // assign to the latest config Shards
	conf.Groups = map[int][]string{}
	for k, v := range latestConf.Groups {
		conf.Groups[k] = v
	}
	return conf
}

// make a map from replica group id to the shards they handle.
func (sm *ShardMaster) makeGidToShardsMap(config *Config) map[int][]int {
	gidToShards := make(map[int][]int)
	for shard, gid := range config.Shards {
		gidToShards[gid] = append(gidToShards[gid], shard)
	}
	return gidToShards
}

func (sm *ShardMaster) makeGidSlice(conf *Config) (gids []int) {
	for gid, _ := range conf.Groups {
		gids = append(gids, gid)
	}
	sort.Ints(gids)
	return
}

func (sm *ShardMaster) findGroupToMove(gidToShards map[int][]int, gids []int) (groupWithMaxShards, maxShards, groupWithMinShards, minShards int) {
	groupWithMaxShards, maxShards, groupWithMinShards, minShards = 0, 0, 0, 0
	for _, gid := range gids {
		shards := gidToShards[gid]
		if groupWithMaxShards == 0 || len(gidToShards[groupWithMaxShards]) < len(shards) {
			groupWithMaxShards = gid
			maxShards = len(shards)
		}
		if groupWithMinShards == 0 || len(gidToShards[groupWithMinShards]) > len(shards) {
			groupWithMinShards = gid
			minShards = len(shards)
		}
	}
	return
}

func (sm *ShardMaster) reBalance(conf *Config) {
	gidToShards := sm.makeGidToShardsMap(conf)
	gids := sm.makeGidSlice(conf)
	if len(gids) == 0 {
		// no available replica group to rebalance
		return
	}

	if len(gidToShards[0]) > 0 {
		// there are shards not allocated to replica group , allocate to replica groups evenly
		i := 0
		for _, shard := range gidToShards[0] {
			targetGid := gids[i]
			gidToShards[targetGid] = append(gidToShards[targetGid], shard)
			conf.Shards[shard] = targetGid // update config
			i = (i + 1) % len(gids)
		}
		delete(gidToShards, 0)
	}

	// each time move a shard from replica group with max shards to replica group with min shards.
	// stop when max shards  == min shards +1
	for {
		groupWithMaxShards, maxShards, groupWithMinShards, minShards := sm.findGroupToMove(gidToShards, gids)
		if maxShards-minShards <= 1 {
			break
		}

		shardToMove := gidToShards[groupWithMaxShards][maxShards-1]
		conf.Shards[shardToMove] = groupWithMinShards // move the rear shard from groupWithMaxShards to groupWithMinShards

		gidToShards[groupWithMinShards] = append(gidToShards[groupWithMinShards], shardToMove)
		gidToShards[groupWithMaxShards] = gidToShards[groupWithMaxShards][:maxShards-1]
	}
}

// the new configuration caused by Join operation must be deterministic among all shard master replicas
func (sm *ShardMaster) applyJoin(entry Op) {
	if len(entry.Servers) == 0 {
		return
	}

	conf := sm.newConfig()
	for gid, servers := range entry.Servers {
		conf.Groups[gid] = servers // update current replica groups
	}
	sm.reBalance(conf)
	sm.configs = append(sm.configs, *conf)

	DPrintf("%d finish join req, latest conf %v", sm.me, *conf)
}

// the new configuration caused by Leave operation must be deterministic among all shard master replicas
func (sm *ShardMaster) applyLeave(entry Op) {
	if len(entry.GIDs) == 0 {
		return
	}

	conf := sm.newConfig()
	gidToShards := sm.makeGidToShardsMap(conf)
	// move shards located on groups which will be removed to group 0
	for _, gidToRemove := range entry.GIDs {
		for _, shardToReallocate := range gidToShards[gidToRemove] {
			conf.Shards[shardToReallocate] = 0
		}
		delete(conf.Groups, gidToRemove)
	}

	sm.reBalance(conf)
	sm.configs = append(sm.configs, *conf)
	DPrintf("%d finish leave req, latest conf %v", sm.me, *conf)
}

func (sm *ShardMaster) applyMove(entry Op) {
	conf := sm.newConfig()
	conf.Shards[entry.Shard] = entry.GID
	sm.configs = append(sm.configs, *conf)
	DPrintf("%d finish move req, latest conf %v", sm.me, *conf)
}

// StartServer servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg, 100)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.stopCh = make(chan any, 1)
	sm.ack = make(map[int64]int)
	sm.pendingRequests = make(map[int]chan Op)

	go sm.Run()

	return sm
}
