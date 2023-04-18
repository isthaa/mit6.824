package raft

import (
	"bytes"

	"6.824/labgob"
)
/*
将 Raft 的持久状态保存到稳定存储中，在崩溃和重启后可以检索到它。
有关应持久保存的内容，请参见论文中的图2
*/
func (rf *Raft) persist() {
	data := SerilizeState(rf)
	rf.persister.SaveRaftState(data)
	Debug(dPersist, "S%d Persist States. T%d, votedFor:%d, log: %v", rf.me,
		rf.currentTerm, rf.votedFor, rf.log)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		Debug(dError, "S%d Read Persist Error!", rf.me)
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		Debug(dPersist, "S%d ReadPersist. State: T%d, votedFor%d, log: %v", rf.me,
			rf.currentTerm, rf.votedFor, rf.log)
	}
}
