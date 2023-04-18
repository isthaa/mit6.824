package shardctrler

import (
	"sync"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

type OpType string

const (
	QUERY   OpType = "QUERY"
	JOIN    OpType = "JOIN"
	LEAVE   OpType = "LEAVE"
	MOVE    OpType = "MOVE"
	TIMEOUT        = 100 // set time out to 100 millsecond.
)

type ShardCtrler struct {
	mu        sync.Mutex
	me        int
	rf        *raft.Raft
	applyCh   chan raft.ApplyMsg
	persister *raft.Persister
	// Your data here.

	configs []Config // indexed by config num

	chans                         map[int64]chan OpResult
	client_to_last_process_seq    map[int64]uint64
	client_to_last_process_result map[int64]OpResult
}

type Op struct {
	// Your data here.
	Type    OpType
	GID     []int
	Shard   int
	Num     int
	Servers map[int][]string

	ClientId  int64
	ClientSeq uint64
	ServerSeq int64
}

type OpResult struct {
	Config Config
	Error  Err
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sc.mu.Lock()
	last_process_seq, ok := sc.client_to_last_process_seq[args.ClientId]
	if ok {
		if last_process_seq == args.ClientSeq {
			reply.Err = sc.client_to_last_process_result[args.ClientId].Error
			reply.WrongLeader = false
			sc.mu.Unlock()
			return
		} else if last_process_seq > args.ClientSeq {
			// 这是一个过时的请求，立即返回。
			sc.mu.Unlock()
			return
		}
	}
	sc.mu.Unlock()

	op := Op{
		Type:      JOIN,
		Servers:   args.Servers,
		ClientId:  args.ClientId,
		ClientSeq: args.ClientSeq,
		ServerSeq: nrand(),
	}
	rec_chan := make(chan OpResult, 1)
	sc.mu.Lock()
	sc.chans[op.ServerSeq] = rec_chan
	sc.mu.Unlock()
	if _, _, ok1 := sc.rf.Start(op); ok1 {
		timer := time.After(TIMEOUT * time.Millisecond)
		select {
		case <-timer:
			// timeout!(超时)
			reply.Err = "TIMEOUT"
			reply.WrongLeader = true
		case res := <-rec_chan:
			// 这个操作已经被处理！
			reply.Err = res.Error
			reply.WrongLeader = false
		}
	} else {
		reply.WrongLeader = true
	}
	sc.mu.Lock()
	delete(sc.chans, op.ServerSeq)
	sc.mu.Unlock()

}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	// Your code here.
	sc.mu.Lock()
	last_process_seq, ok := sc.client_to_last_process_seq[args.ClientId]
	if ok {
		if last_process_seq == args.ClientSeq {
			reply.Err = sc.client_to_last_process_result[args.ClientId].Error
			reply.WrongLeader = false
			sc.mu.Unlock()
			return
		} else if last_process_seq > args.ClientSeq {
			// this is a out of date request, return immediately
			sc.mu.Unlock()
			return
		}
	}
	sc.mu.Unlock()

	op := Op{
		Type:      LEAVE,
		GID:       args.GIDs,
		ClientId:  args.ClientId,
		ClientSeq: args.ClientSeq,
		ServerSeq: nrand(),
	}
	rec_chan := make(chan OpResult, 1)
	sc.mu.Lock()
	sc.chans[op.ServerSeq] = rec_chan
	sc.mu.Unlock()
	if _, _, ok1 := sc.rf.Start(op); ok1 {
		timer := time.After(TIMEOUT * time.Millisecond)
		select {
		case <-timer:
			// timeout!
			reply.Err = "TIMEOUT"
			reply.WrongLeader = true
		case res := <-rec_chan:
			// this op has be processed!
			reply.Err = res.Error
			reply.WrongLeader = false
		}
	} else {
		reply.WrongLeader = true
	}
	sc.mu.Lock()
	delete(sc.chans, op.ServerSeq)
	sc.mu.Unlock()
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	// Your code here.
	sc.mu.Lock()
	last_process_seq, ok := sc.client_to_last_process_seq[args.ClientId]
	if ok {
		if last_process_seq == args.ClientSeq {
			reply.Err = sc.client_to_last_process_result[args.ClientId].Error
			reply.WrongLeader = false
			sc.mu.Unlock()
			return
		} else if last_process_seq > args.ClientSeq {
			// this is a out of date request, return immediately
			sc.mu.Unlock()
			return
		}
	}
	sc.mu.Unlock()

	op := Op{
		Type:      MOVE,
		Shard:     args.Shard,
		GID:       make([]int, 1),
		ClientId:  args.ClientId,
		ClientSeq: args.ClientSeq,
		ServerSeq: nrand(),
	}
	op.GID[0] = args.GID
	rec_chan := make(chan OpResult, 1)
	sc.mu.Lock()
	sc.chans[op.ServerSeq] = rec_chan
	sc.mu.Unlock()
	if _, _, ok1 := sc.rf.Start(op); ok1 {
		timer := time.After(TIMEOUT * time.Millisecond)
		select {
		case <-timer:
			// timeout!
			reply.Err = "TIMEOUT"
			reply.WrongLeader = true
		case res := <-rec_chan:
			// this op has be processed!
			reply.Err = res.Error
			reply.WrongLeader = false
		}
	} else {
		reply.WrongLeader = true
	}
	sc.mu.Lock()
	delete(sc.chans, op.ServerSeq)
	sc.mu.Unlock()
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	// Your code here.
	sc.mu.Lock()
	last_process_seq, ok := sc.client_to_last_process_seq[args.ClientId]
	if ok {
		if last_process_seq == args.ClientSeq {
			reply.Err = sc.client_to_last_process_result[args.ClientId].Error
			reply.WrongLeader = false
			sc.mu.Unlock()
			return
		} else if last_process_seq > args.ClientSeq {
			// this is a out of date request, return immediately
			sc.mu.Unlock()
			return
		}
	}
	sc.mu.Unlock()

	op := Op{
		Type:      QUERY,
		Num:       args.Num,
		ClientId:  args.ClientId,
		ClientSeq: args.ClientSeq,
		ServerSeq: nrand(),
	}
	rec_chan := make(chan OpResult, 1)
	sc.mu.Lock()
	sc.chans[op.ServerSeq] = rec_chan
	sc.mu.Unlock()
	if _, _, ok1 := sc.rf.Start(op); ok1 {
		timer := time.After(TIMEOUT * time.Millisecond)
		select {
		case <-timer:
			// timeout!
			reply.Err = "TIMEOUT"
			reply.WrongLeader = true
		case res := <-rec_chan:
			// this op has be processed!
			reply.Err = res.Error
			reply.WrongLeader = false
			reply.Config = res.Config
		}
	} else {
		reply.WrongLeader = true
	}
	sc.mu.Lock()
	delete(sc.chans, op.ServerSeq)
	sc.mu.Unlock()
}

/*
当不再需要 ShardCtrler 实例时，测试程序会调用 Kill() 方法。
在 Kill() 方法中，您不需要执行任何操作，但可能会方便一些（例如）
关闭此实例的调试输出。
*/
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) update(op Op, res OpResult) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.client_to_last_process_seq[op.ClientId] = op.ClientSeq
	sc.client_to_last_process_result[op.ClientId] = res
}

// check whether need to process(处理)
func (sc *ShardCtrler) check_dup(op Op) (bool, OpResult) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	client_last_process_seq, ok := sc.client_to_last_process_seq[op.ClientId]
	if ok {
		if op.ClientSeq <= client_last_process_seq {
			// need not to process(不需要处理)
			return false, sc.client_to_last_process_result[op.ClientId]
		}
	}
	// need to process
	return true, OpResult{}
}

func (sc *ShardCtrler) replyChan(server_seq int64, res OpResult) {
	sc.mu.Lock()
	send_chan, ok := sc.chans[server_seq]
	sc.mu.Unlock()
	if ok {
		send_chan <- res
	}
}

func (sc *ShardCtrler) process() {
	for command := range sc.applyCh {
		if command.CommandValid {
			op := command.Command.(Op)
			need_process, res := sc.check_dup(op)
			if !need_process {
				sc.replyChan(op.ServerSeq, res)
				continue
			}
			switch op.Type {
			case QUERY:
				res := OpResult{}
				res_idx := op.Num
				if op.Num == -1 || op.Num >= len(sc.configs) {
					res_idx = len(sc.configs) - 1
				}
				res.Config = sc.configs[res_idx]
				res.Error = OK
				sc.update(op, res)
				sc.replyChan(op.ServerSeq, res)
			case JOIN:
				res := OpResult{}
				new_config := CopyConfig(&sc.configs[len(sc.configs)-1])
				for k, v := range op.Servers {
					new_config.Groups[k] = v
				}
				new_config.ReAllocGID()
				sc.configs = append(sc.configs, new_config)
				res.Error = OK
				sc.update(op, res)
				sc.replyChan(op.ServerSeq, res)
			case LEAVE:
				res := OpResult{}
				new_config := CopyConfig(&sc.configs[len(sc.configs)-1])
				for _, i := range op.GID {
					delete(new_config.Groups, i)
				}
				new_config.ReAllocGID()
				sc.configs = append(sc.configs, new_config)
				res.Error = OK
				sc.update(op, res)
				sc.replyChan(op.ServerSeq, res)
			case MOVE:
				res := OpResult{}
				new_config := CopyConfig(&sc.configs[len(sc.configs)-1])
				new_config.Shards[op.Shard] = op.GID[0]
				sc.configs = append(sc.configs, new_config)
				res.Error = OK
				sc.update(op, res)
				sc.replyChan(op.ServerSeq, res)
			default:
				Debug(dError, "S%d ShardCtrler Process Unknown OP: %v", sc.me, op)
			}
		} else if command.SnapshotValid {
			Debug(dError, "S%d ShardCtrler Should not Receive SnapShot!", sc.me)
		} else {
			Debug(dError, "S%d ShardCtrler Process Unknown Command: %v", sc.me, command)
		}
	}
}

/*
servers[] 数组包含一组服务器的端口号，这些服务器将通过 Raft 协作
形成容错的 shardctrler 服务。me 是当前服务器在 servers[] 数组中的索引。
*/
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.

	sc.chans = make(map[int64]chan OpResult)
	sc.client_to_last_process_seq = make(map[int64]uint64)
	sc.client_to_last_process_result = make(map[int64]OpResult)

	go sc.process()
	return sc
}
