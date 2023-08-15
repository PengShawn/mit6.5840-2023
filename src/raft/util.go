package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

// 最小值min
func min(num int, num1 int) int {
	if num > num1 {
		return num1
	} else {
		return num
	}
}

// 通过不同的随机种子生成不同的过期时间
func generateOverTime(server int64) int {
	rand.Seed(time.Now().Unix() + server)
	return rand.Intn(MoreVoteTime) + MinVoteTime
}

// UpToDate paper中投票RPC的rule2
func (rf *Raft) UpToDate(index int, term int) bool {
	lastIndex := rf.getLastIndex()
	lastTerm := rf.getLastTerm()
	return term > lastTerm || (term == lastTerm && index >= lastIndex)
}

// 获取最后的日志下标
func (rf *Raft) getLastIndex() int {
	return len(rf.logs) - 1
}

// 获取最后的日志任期
func (rf *Raft) getLastTerm() int {
	// 因为初始有填充一个，所以初始长度为1
	if len(rf.logs) == 1 {
		return 0
	} else {
		return rf.logs[len(rf.logs)-1].Term
	}
}

func (rf *Raft) restoreLogTerm(curIndex int) int {
	if curIndex <= 0 {
		return 0
	}
	if curIndex >= len(rf.logs) {
		return 0
	}
	return rf.logs[curIndex].Term
}

func (rf *Raft) getPrevLogInfo(server int) (int, int) {
	newEntryBeginIndex := rf.nextIndex[server] - 1
	lastIndex := rf.getLastIndex()
	if newEntryBeginIndex == lastIndex+1 {
		newEntryBeginIndex = lastIndex
	}
	return newEntryBeginIndex, rf.restoreLogTerm(newEntryBeginIndex)
}

// 通过快照偏移还原真实日志条目
func (rf *Raft) restoreLog(curIndex int) LogEntry {
	return rf.logs[curIndex]
}
