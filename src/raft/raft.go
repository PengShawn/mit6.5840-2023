package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"mit6.5840-2023/labgob"
	"mit6.5840-2023/labrpc"
)

//节点角色
type NodeState int

const (
	Follower NodeState = iota
	Candidate
	Leader
)

const (
	// 随机生成投票过期时间范围:(MinVoteTime~MoreVoteTime+MinVoteTime)
	MinVoteTime = 150
	MoreVoteTime = 200
	HeartbeatSleep = 120	// 心跳时间
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// 日志条目
type LogEntry struct {
	Term int				// 指令收到时的任期号
	Command interface{}		// ⽤户状态机执⾏的指令
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	currentTerm int 			// 当前任期
	votedFor    int 			// 当前任期对其投票的candidateId
	votedTime time.Time 		// 投票的时间
	logs		[]LogEntry 		// 日志条目集，每⼀个条⽬包含⼀个⽤户状态机执⾏的指令，和收到时的任期号

	// 所有服务器上经常变的
	commitIndex int // 已知的最⼤的已经被提交的⽇志条⽬的索引值
	lastApplied int // 最后被应⽤到状态机的⽇志条⽬索引值

	// leader拥有的可见变量，用来管理他的follower(leader经常修改的）
	nextIndex  []int // 对于每⼀个服务器，需要发送给他的下⼀个⽇志条⽬的索引值（初始化为领导⼈最后索引值加⼀）
	matchIndex []int // 对于每⼀个服务器，已经复制给他的⽇志的最⾼索引值

	nodeState NodeState		// 节点角色
	voteNum   int			// 当前票数
	applyChan chan ApplyMsg // 日志都是存在这里client取
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.nodeState == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int // 候选⼈的任期号
	CandidateId  int // 请求选票的候选⼈的 Id
	LastLogIndex int // 候选⼈的最后⽇志条⽬的索引值
	LastLogTerm  int // 候选⼈最后⽇志条⽬的任期号
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int       // 投票方的term，以便于候选⼈去更新⾃⼰的任期号
	VoteGranted bool      // 是否投票给了该竞选人
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 网络分区或节点crash导致任期比投票者还要小，直接返回
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.nodeState = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.voteNum = 0
	}
	if !rf.UpToDate(args.LastLogIndex, args.LastLogTerm) ||
		rf.votedFor != -1 && rf.votedFor != args.CandidateId && args.Term == reply.Term {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	rf.votedFor = args.CandidateId
	rf.currentTerm = args.Term
	rf.votedTime = time.Now()
	reply.VoteGranted = true
	reply.Term = rf.currentTerm
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//追加待同步⽇志；由 Leader 负责调⽤来复制⽇志，也会⽤作心跳
type AppendEntriesArgs struct {
	Term 			int 		// 领导⼈的任期号
	LeaderId 		int			// 领导⼈的 Id，以便于跟随者重定向请求
	PrevLogIndex 	int			// 新的⽇志条⽬紧随之前的索引值
	PrevLogTerm 	int			// prevLogIndex 条⽬的任期号
	Entries 		[]LogEntry  // 准备存储的⽇志条⽬（表示⼼跳时为空；⼀次性发送多个是为了提⾼效率）
	LeaderCommit 	int			// 领导⼈已经提交的⽇志的索引值
}

type AppendEntriesReply struct {
	Term 		int					// 当前的任期号，⽤于领导⼈去更新⾃⼰
	Success 	bool				// 跟随者包含了匹配上 prevLogIndex 和 prevLogTerm 的⽇志时为真
	UpNextIndex int				    // 如果发生冲突时，reply传过来的正确的下标用于更新nextIndex[i] 
}

//同步日志/建立心跳
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 网络分区导致args的任期比当前节点小
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.UpNextIndex = -1
		return
	}

	reply.Term = args.Term
	reply.Success = true
	reply.UpNextIndex = -1

	rf.currentTerm = args.Term
	rf.votedFor = -1
	rf.nodeState = Follower
	rf.voteNum = 0
	rf.votedTime = time.Now()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) leaderAppendEntries() {
	for index := range rf.peers {
		if index == rf.me {
			continue
		}

		// 开启协程并发的进行日志增量
		go func(server int) {
			rf.mu.Lock()
			if rf.nodeState != Leader {
				rf.mu.Unlock()
				return
			}

			prevLogIndex, prevLogTerm := rf.getPrevLogInfo(server)
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				LeaderCommit: rf.commitIndex,
			}
			args.Entries = []LogEntry{}
			reply := AppendEntriesReply{}
			rf.mu.Unlock()

			re := rf.sendAppendEntries(server, &args, &reply)

			if re {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if rf.nodeState != Leader {
					return
				}

				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.nodeState = Follower
					rf.votedFor = -1
					rf.voteNum = 0
					rf.persist()
					rf.votedTime = time.Now()
					return
				}
			}

		}(index)

	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// 发起选举，请求投票
func (rf *Raft) sendElection() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		// 开启协程对各个节点发起选举
		go func(server int) {
			rf.mu.Lock()
			args := RequestVoteArgs{
				rf.currentTerm,
				rf.me,
				rf.getLastIndex(),
				rf.getLastTerm(),
			}
			reply := RequestVoteReply{}
			rf.mu.Unlock()
			res := rf.sendRequestVote(server, &args, &reply)

			if res {
				rf.mu.Lock()
				// 判断自身是否还是竞选者，且任期不冲突
				if rf.nodeState != Candidate || args.Term < rf.currentTerm {
					rf.mu.Unlock()
					return
				}

				// 返回者的任期大于args（网络分区原因)进行返回
				if reply.Term > args.Term {
					if rf.currentTerm < reply.Term {
						rf.currentTerm = reply.Term
					}
					rf.nodeState = Follower
					rf.votedFor = -1
					rf.voteNum = 0
					rf.mu.Unlock()
					return
				}

				// 返回结果正确判断是否大于一半节点同意
				if reply.VoteGranted && rf.currentTerm == args.Term {
					rf.voteNum += 1
					if rf.voteNum >= (len(rf.peers)/2 + 1) {
						rf.nodeState = Leader
						rf.votedFor = -1
						rf.voteNum = 0

						rf.nextIndex = make([]int, len(rf.peers))
						for i := 0; i < len(rf.peers); i++ {
							rf.nextIndex[i] = rf.getLastIndex() + 1
						}

						rf.matchIndex = make([]int, len(rf.peers))
						rf.matchIndex[rf.me] = rf.getLastIndex()
						rf.votedTime = time.Now()
						rf.mu.Unlock()
						return
					}
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()
				return
			}
		}(i)
	}
}

// 发起选举计时器
func (rf *Raft) electionTicker() {
	for !rf.killed() {
		preTime := time.Now()
		time.Sleep(time.Duration(generateOverTime(int64(rf.me))) * time.Millisecond)
		rf.mu.Lock()
		if rf.votedTime.Before(preTime) && rf.nodeState != Leader {
			rf.nodeState = Candidate
			rf.votedFor = rf.me
			rf.voteNum = 1
			rf.currentTerm += 1
			rf.votedTime = time.Now()
			rf.sendElection()
		}
		rf.mu.Unlock()
	}
}

// 追加日志计时器
func (rf *Raft) appendTicker() {
	for !rf.killed() {
		time.Sleep(HeartbeatSleep * time.Millisecond)
		rf.mu.Lock()
		if rf.nodeState == Leader {
			rf.mu.Unlock()
			rf.leaderAppendEntries()
		} else {
			rf.mu.Unlock()
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.mu.Lock()
	rf.nodeState = Follower
	rf.currentTerm = 0
	rf.voteNum = 0
	rf.votedFor = -1
	rf.lastApplied = 0
	rf.commitIndex = 0
	rf.applyChan = applyCh

	rf.logs = []LogEntry{}
	rf.logs = append(rf.logs, LogEntry{})
	rf.mu.Unlock()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.electionTicker()
	go rf.appendTicker()

	return rf
}
