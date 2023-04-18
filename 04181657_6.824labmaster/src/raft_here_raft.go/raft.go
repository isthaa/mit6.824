package raft

/*
这是Raft必须向服务（或测试器）公开的API概述。请参见下面的注释以获取更多详细信息。

rf = Make(...)
创建一个新的Raft服务器。
rf.Start(command interface{}) (index, term, isleader)
开始就一个新的日志条目达成一致。
rf.GetState() (term, isLeader)
询问Raft其当前任期以及它是否认为自己是领导者。
ApplyMsg
每次将新条目提交到日志时，每个Raft对等方都应该向服务（或测试器）发送一个ApplyMsg，
位于同一服务器上。
*/

import (
	//	"bytes"

	"sync"
	"sync/atomic"
	"time"

	"6.824/labrpc"
)


/*
当每个Raft节点意识到连续的日志条目已经被提交时，
节点应该通过传递给Make()的applyCh向同一服务器上的服务（或测试器）发送一个ApplyMsg。
将CommandValid设置为true，表示ApplyMsg包含一个新提交的日志条目。

在第2D部分中，您将想要在applyCh上发送其他类型的消息（例如，快照），
但对于这些其他用途，将CommandValid设置为false。
*/


const (
	LEADER    = 0
	CANDIDATE = 1
	FOLLOWER  = 2
)

var roler_string map[int]string

const (
	ELECTION_TIMER_RESOLUTION = 5 //每5毫秒检查一次计时器是否已过期
	// 投票过期时间范围（毫秒）
	ELECTION_EXPIRE_LEFT  = 200
	ELECTION_EXPIRE_RIGHT = 400
	// heartbeat time (millsecond)
	APPEND_EXPIRE_TIME      = 100
	APPEND_TIMER_RESOLUTION = 2
)

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}


// 一个实现单个Raft节点的Go对象

type Raft struct {
	mu sync.Mutex // 用于保护共享访问此节点状态的锁
	cv *sync.Cond // 同步生产者和消费者的条件变量

	peers     []*labrpc.ClientEnd // 所有节点的RPC终端点
	persister *Persister          // 用于保存此节点持久化状态的对象
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	currentTerm    int
	votedFor       int // -1 表示 null
	receiveVoteNum int
	log            []LogEntry

	roler int

	ElectionExpireTime time.Time   // election 过期 time
	AppendExpireTime   []time.Time // next send append time

	
	// 已知已提交的最高日志条目的索引
	// （初始化为0，单调递增）
	commitIdx int

	// AppendEntries RPC应该将nextIndex[peer_i]日志发送给peer_i。
	nextIndex []int

	// matchIndex[i]表示peer_i和此领导者匹配的日志是[1-matchIndex[i]]
	// matchIndex随着nextIndex的变化而变化，并影响commitIndex的更新。
	matchIndex []int

	// 异步应用已提交的日志或快照！
	commitQueue []ApplyMsg
}

// 返回currentTerm和此服务器是否认为自己是领导者。
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isleader := rf.roler == LEADER
	DebugGetInfo(rf)
	return term, isleader
}

// get the first 虚拟 log index
func (rf *Raft) getFirstIndex() int {
	return rf.log[0].Index
}

// get the first 虚拟 log term
func (rf *Raft) getFirstTerm() int {
	return rf.log[0].Term
}

// get the last log term
func (rf *Raft) getLastTerm() int {
	return rf.log[len(rf.log)-1].Term
}

// get the last log index
func (rf *Raft) getLastIndex() int {
	return rf.log[len(rf.log)-1].Index
}

// get the Term of index
// compute the location in log and return the result
func (rf *Raft) getTermForIndex(index int) int {
	return rf.log[index-rf.getFirstIndex()].Term
}

// get the command of index
// compute the location in log and return the result
func (rf *Raft) getCommand(index int) interface{} {
	return rf.log[index-rf.getFirstIndex()].Command
}



/*
使用Raft的服务（例如k/v服务器）希望在下一个要附加到Raft日志的命令上达成一致。 
如果此服务器不是领导者，则返回false。否则开始达成一致并立即返回。不能保证这个
命令将被提交到Raft日志中，因为领导者可能会失败或输掉选举。即使Raft实例已被终止，
此函数也应正常返回。

第一个返回值是命令将出现的索引（如果它被提交）。第二个返回值是当前术语。如果此服务器认为它是
领导者，则第三个返回值为true。
*/


func (rf *Raft) Start(command interface{}) (int, int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() {
		return -1, -1, false
	}
	if rf.roler != LEADER {
		return -1, -1, false
	}

	// 这是一个活跃的领导者，
	// 将此命令附加到其日志中并返回
	// HBT计时器将同步此日志到其他对等方
	DebugNewCommand(rf)
	rf.log = append(rf.log, LogEntry{rf.currentTerm, rf.getLastIndex() + 1, command})
	rf.persist()
	// 立即发送附加日志条目请求
	for i := 0; i < len(rf.peers); i++ {
		rf.ResetAppendTimer(i, true)
	}
	return rf.getLastIndex(), rf.currentTerm, true
}

/*
测试程序在每个测试后不会停止 Raft 创建的 goroutine，但它会调用 Kill() 方法。
您的代码可以使用 killed() 方法来检查是否已调用 Kill()。使用原子变量可以避免使用锁。

问题在于长时间运行的 goroutine 会使用内存并可能占用 CPU 时间，导致后续测试失败并生成混乱的调试输出。
任何具有长时间运行循环的 goroutine 应该调用 killed() 来检查是否应该停止。
*/
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

/*
服务或测试程序想要创建一个 Raft 服务器。所有 Raft 服务器（包括此服务器）的端口都在 peers[] 中。此服务器的端口是 peers[me]。
所有服务器的 peers[] 数组顺序相同。 persister 是服务器保存其持久状态的位置，并且如果有的话，最初还保存了最近保存的状态。applyCh 是一个通道，
测试程序或服务期望 Raft 发送 ApplyMsg 消息到其中。Make() 必须快速返回，因此应为任何长时间运行的工作启动 goroutine。
*/
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {

	roler_string = map[int]string{
		LEADER:    "L",
		CANDIDATE: "C",
		FOLLOWER:  "F",
	}

	num_servers := len(peers)

	rf := &Raft{
		peers:            peers,
		persister:        persister,
		me:               me,
		currentTerm:      0,
		votedFor:         -1,
		log:              make([]LogEntry, 0),
		roler:            FOLLOWER,
		AppendExpireTime: make([]time.Time, num_servers),
		nextIndex:        make([]int, num_servers),
		matchIndex:       make([]int, num_servers),
		commitQueue:      make([]ApplyMsg, 0),
	}
	rf.cv = sync.NewCond(&rf.mu)
	// add a 虚拟 log
	rf.log = append(rf.log, LogEntry{
		Index:   0,
		Term:    0,
		Command: nil,
	})
	rf.ResetElectionTimer()
	for i := 0; i < num_servers; i++ {
		rf.ResetAppendTimer(i, false)
	}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// initialize commitIndex after recover
	rf.commitIdx = rf.getFirstIndex()

	// start ticker goroutine to check Election and appendEntry
	go rf.election_ticker()
	go rf.append_ticker()

	// 开始异步应用 goroutine
	go rf.Applier(applyCh)

	Debug(dInfo, "Start S%d", me)

	return rf
}
