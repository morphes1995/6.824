package raftkv

import (
	"6.824/src/labgob"
	"6.824/src/labrpc"
	"6.824/src/raft"
	"log"
	"sync"
	"time"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const (
	GET    = "GET"
	PUT    = "PUT"
	APPEND = "APPEND"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Command   string // only support get or put or append
	Key       string
	Value     string
	ClientId  int64
	RequestId int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	stopCh  chan any

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	data            map[string]string
	ack             map[int64]int
	pendingRequests map[int]chan Op // todo each op has a chan, wasteful !!
}

func (kv *KVServer) Run() {
	for {
		select {
		case msg := <-kv.applyCh:
			entry := msg.Command.(Op)

			kv.mu.Lock()
			requestId, ok := kv.ack[entry.ClientId]
			if !ok || requestId < entry.RequestId {
				kv.applyEntry(entry)
				kv.ack[entry.ClientId] = entry.RequestId
			}

			ch, ok := kv.pendingRequests[msg.CommandIndex]
			if ok {
				ch <- entry
			}

			kv.mu.Unlock()

		case <-kv.stopCh:
			return

		}
	}
}

func (kv *KVServer) applyEntry(entry Op) {
	switch entry.Command {
	case PUT:
		kv.data[entry.Key] = entry.Value
	case APPEND:
		kv.data[entry.Key] += entry.Value
	case GET:

	}

}

func (kv *KVServer) appendLogEntry(entry Op) bool {
	kv.mu.Lock()
	requestId, ok := kv.ack[entry.ClientId]
	if ok && requestId >= entry.RequestId { // ignore requests that already processed
		kv.mu.Unlock()
		DPrintf("request %d already processed", entry.RequestId)
		return true
	}
	kv.mu.Unlock()

	index, _, isLeader := kv.rf.Start(entry)
	if !isLeader {
		return false
	}

	kv.mu.Lock()
	ch, ok := kv.pendingRequests[index]
	if !ok {
		ch = make(chan Op, 1)
		kv.pendingRequests[index] = ch
	}
	kv.mu.Unlock()

	// wait for raft to commit and apply this op
	result := false
	select {
	case entryApplied := <-ch:
		// logs[index] may be changed and is not equal to `entry` , because the leadership change
		// and old leader's log entry was overwritten by new leader's
		result = entryApplied == entry
	case <-time.After(300 * time.Millisecond):
	}

	kv.mu.Lock()
	delete(kv.pendingRequests, index) // remove stale chan
	kv.mu.Unlock()

	return result
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	entry := Op{
		Command:   GET,
		Key:       args.Key,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}

	ok := kv.appendLogEntry(entry)
	if !ok {
		reply.WrongLeader = true
		return
	}

	reply.WrongLeader = false
	reply.Err = OK
	kv.mu.Lock()
	reply.Value = kv.data[args.Key]
	kv.mu.Unlock()

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	entry := Op{
		Command:   args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}

	ok := kv.appendLogEntry(entry)
	if !ok {
		reply.WrongLeader = true
		return
	}

	reply.WrongLeader = false
	reply.Err = OK
}

// Kill the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	kv.stopCh <- true
}

// StartKVServer
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.stopCh = make(chan any, 1)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.data = make(map[string]string)
	kv.ack = make(map[int64]int)
	kv.pendingRequests = make(map[int]chan Op)

	go kv.Run()

	return kv
}
