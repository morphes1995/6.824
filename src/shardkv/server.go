package shardkv

// import "shardmaster"
import (
	"6.824/src/labgob"
	"6.824/src/labrpc"
	"6.824/src/raft"
	"6.824/src/shardmaster"
	"bytes"
	"sync"
	"time"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	ClientId  int64
	RequestId int
	Command   string

	Key   string
	Value string
}

type OpResult struct {
	Op
	Err Err
}

//type Result struct {
//	Command     string
//	OK          bool
//	ClientId    int64
//	RequestId   int64
//	WrongLeader bool
//	Err         Err
//	Value       string
//	// ReconfigureReply
//	Num int
//}

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
	stopCh          chan any
	data            [shardmaster.NShards]map[string]string // each shard has a kv map
	ack             map[int64]int
	pendingRequests map[int]chan OpResult

	config shardmaster.Config
	mck    *shardmaster.Clerk
}

func (kv *ShardKV) own(key string) bool {
	return kv.config.Shards[key2shard(key)] == kv.gid
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	entry := Op{
		Command:   Get,
		Key:       args.Key,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}

	ok, err := kv.appendLogEntry(entry)
	reply.WrongLeader = err == WrongLeader
	reply.Err = err
	if !ok {
		return
	}

	kv.mu.Lock()
	shard := key2shard(args.Key)
	if value, exists := kv.data[shard][args.Key]; exists {
		reply.Value = value
		reply.Err = OK
	} else {
		reply.Err = ErrNoKey
	}

	kv.mu.Unlock()
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	entry := Op{
		Command:   args.Op,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		Key:       args.Key,
		Value:     args.Value,
	}

	_, err := kv.appendLogEntry(entry)
	reply.WrongLeader = err == WrongLeader
	reply.Err = err
}

// check if the result corresponds to the log entry.
func (kv *ShardKV) isMatch(entry1 Op, entry2 Op) bool {
	return entry1.ClientId == entry2.ClientId && entry1.RequestId == entry2.RequestId
}

func (kv *ShardKV) appendLogEntry(entry Op) (bool, Err) {
	//kv.mu.Lock()
	//requestId, ok := kv.ack[entry.ClientId]
	//if ok && requestId >= entry.RequestId { // ignore requests that already processed
	//	kv.mu.Unlock()
	//	DPrintf("request %d already processed", entry.RequestId)
	//	return true,OK
	//}
	//kv.mu.Unlock()

	index, _, isLeader := kv.rf.Start(entry)
	if !isLeader {
		return false, WrongLeader
	}

	kv.mu.Lock()
	ch, ok := kv.pendingRequests[index]
	if !ok {
		ch = make(chan OpResult, 1)
		kv.pendingRequests[index] = ch
	}
	kv.mu.Unlock()

	// wait for raft to commit and apply this op
	result, err := false, WrongLeader
	select {
	case entryRes := <-ch:
		// logs[index] may be changed and is not equal to `entry` , because the leadership change
		// and old leader's log entry was overwritten by new leader's
		if entryRes.Op == entry {
			result, err = true, OK
		}
	case <-time.After(300 * time.Millisecond):
		DPrintf("%v wait appendLogEntry timeout ", kv.me)
	}

	kv.mu.Lock()
	if !kv.own(entry.Key) {
		result, err = false, ErrWrongGroup // this kv server is not the owner of the shard that contains the key
	}
	delete(kv.pendingRequests, index) // remove stale chan
	kv.mu.Unlock()

	DPrintf("%v finish appendLogEntry ,op %v,  result %v , err %v ", kv.me, entry.Command, result, err)
	return result, Err(err)
}

func (kv *ShardKV) Run() {
	for {
		select {
		// 0. receive applied log entry from raft
		case msg := <-kv.applyCh:
			kv.mu.Lock()
			if msg.IsSnapshot {
				// 1. apply snapshot
				kv.applySnapshot(msg.Snapshot)
			} else {
				entry := msg.Command.(Op)

				err := ""
				if kv.own(entry.Key) {
					requestId, ok := kv.ack[entry.ClientId]
					if !ok || requestId < entry.RequestId {
						// 2.1. apply state to kv server
						kv.applyEntry(entry)
						kv.ack[entry.ClientId] = entry.RequestId
					}
				} else {
					err = ErrWrongGroup
				}

				// 2.2. notify pending operation
				ch, ok := kv.pendingRequests[msg.CommandIndex]
				if ok {
					ch <- OpResult{Op: entry, Err: Err(err)}
				}

				// 2.3. create snapshot if raft state exceeds allowed size
				if kv.maxraftstate != -1 && kv.rf.GetStateSize() >= kv.maxraftstate {
					w := new(bytes.Buffer)
					e := labgob.NewEncoder(w)

					e.Encode(kv.data)
					e.Encode(kv.ack)
					e.Encode(kv.config)
					go kv.rf.CreateSnapShot(w.Bytes(), msg.CommandIndex)
				}
			}
			kv.mu.Unlock()

		case <-kv.stopCh:
			return

		}
	}
}

func (kv *ShardKV) Reconfigure() {
	for {
		kv.config = kv.mck.Query(-1)
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) applyEntry(entry Op) {
	switch entry.Command {
	case Get:
		// do nothing
	case Put:
		kv.data[key2shard(entry.Key)][entry.Key] = entry.Value
	case Append:
		kv.data[key2shard(entry.Key)][entry.Key] += entry.Value
	}
}

func (kv *ShardKV) applySnapshot(snapshot []byte) {
	var lastIncludedIndex, lastIncludedTerm int
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	d.Decode(&lastIncludedIndex)
	d.Decode(&lastIncludedTerm)
	d.Decode(&kv.data)
	d.Decode(&kv.ack)
	d.Decode(&kv.config)
}

// Kill the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	kv.stopCh <- true
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

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.mck = shardmaster.MakeClerk(masters)

	for i := 0; i < shardmaster.NShards; i++ {
		kv.data[i] = make(map[string]string)
	}

	kv.stopCh = make(chan any, 1)
	kv.applyCh = make(chan raft.ApplyMsg, 100)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.ack = make(map[int64]int)
	kv.pendingRequests = make(map[int]chan OpResult)

	go kv.Run()
	go kv.Reconfigure()

	return kv
}
