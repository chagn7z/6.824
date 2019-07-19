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

import "sync"
import "labrpc"
import "fmt"
import "math/rand"
import "time"
import "sync/atomic"
// import "math"

// import "bytes"
// import "labgob"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Term int
	Command interface{}
}

const (
	nFollower  = 0
	nCandidate = 1
	nLeader    = 2
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	role int

	currentTerm int
	votedFor int
	log []LogEntry

	commitIndex int
	lastApplied int

	// For leader
	nextIndex []int
	matchIndex []int

	isSync []bool

	// For follower
	electionTimer int

	// debug
	counter int
	applyCh chan ApplyMsg
}

func (rf *Raft) ShowInfo() {
	// var log  []LogEntry
	// if len(rf.log) < 4 {
	// 	log = rf.log
	// } else {
	// 	log = rf.log[:4]
	// }
	
	fmt.Printf("[%p %d] role %d, term %d, vote %d, commit %v, isSync %v, nextIndex %v, matchIndex %v, log %v\n",
		rf, rf.me, rf.role, rf.currentTerm, rf.votedFor, rf.commitIndex, rf.isSync, rf.nextIndex, rf.matchIndex, rf.log)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).

	rf.mu.Lock()
	term = rf.currentTerm
	if rf.role == nLeader {
		isleader = true
	} else {
		isleader = false
	}
	rf.mu.Unlock()

	// fmt.Printf("[-][%d] %v %v\n", rf.me, term, isleader)

	return term, isleader
}


//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
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




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		rf.currentTerm = args.Term
	} else {
		if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
			lastLogTerm := rf.log[len(rf.log)-1].Term
			lastLogIndex := len(rf.log) - 1
			if args.LastLogTerm > lastLogTerm {
				rf.votedFor = args.CandidateId
				reply.VoteGranted = true
			} else if args.LastLogTerm == lastLogTerm {
				if args.LastLogIndex >= lastLogIndex {
					rf.votedFor = args.CandidateId
					reply.VoteGranted = true
				}
			}
		}
	}
	reply.Term = rf.currentTerm
	// fmt.Printf("[%d] candidate %d, term %d, RequestVote Args %v, Reply %v\n", rf.me, args.CandidateId, rf.currentTerm, *args, *reply)
	// rf.ShowInfo()
}

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int

	Entries []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool
	FollowerCommit int
}

// 1.heartbeat
// 2.replicate log entries
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// fmt.Printf("[%p %d] role %d, leader %d, term %d, AppendEntries Args %v\n", rf, rf.me, rf.role, args.LeaderId, rf.currentTerm, *args)

	if rf.role == nLeader {
		if args.Term > rf.currentTerm {
			rf.role = nFollower
			rf.currentTerm = args.Term
		}
	}

	if rf.role == nCandidate {
		rf.role = nFollower
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
		}
	}

	if rf.role == nFollower {
		rf.electionTimer = 0

		reply.Term = rf.currentTerm
		reply.Success = false
		if args.Term >= rf.currentTerm {
			rf.currentTerm = args.Term
			if len(args.Entries) > 0 {
				if len(rf.log) == 1 {
					if args.PrevLogIndex == 0 {
						reply.Success = true
						rf.log = append(rf.log, args.Entries...)
					}
				} else if args.PrevLogIndex < len(rf.log) {
					if rf.log[args.PrevLogIndex].Term == args.PrevLogTerm {
						reply.Success = true
						rf.log = append(rf.log, args.Entries...)
					} else {
						rf.log = rf.log[:args.PrevLogIndex]
					}
				}
			} else {
				reply.Success = false
				if len(rf.log) == 1 {
					reply.Success = true
				} else {
					// check follower log sync
					lastLogIndex := len(rf.log) - 1
					if args.PrevLogIndex <= lastLogIndex {
						if rf.log[lastLogIndex].Term == args.Term {
							reply.Success = true

							commit_id := 0
							if (len(rf.log) - 1) > args.LeaderCommit {
								commit_id = args.LeaderCommit
							} else {
								commit_id = len(rf.log) - 1
							}

							for i := rf.commitIndex+1; i < commit_id+1; i++ {
								// fmt.Println("[",rf.me,"] sub", ApplyMsg{true, rf.log[i].Command, i})
								rf.applyCh <- ApplyMsg{true, rf.log[i].Command, i}
								rf.commitIndex++
							}
						} else {
							rf.log = rf.log[:args.PrevLogIndex]
						}
					}
				}
			}
		}
	}

	// fmt.Printf("[%p %d] leader %d, term %d, AppendEntries Args %v, Reply %v\n", rf, rf.me, args.LeaderId, rf.currentTerm, *args, *reply)
	// rf.ShowInfo()
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Your code here (2B).
	if rf.role != nLeader {
		term = rf.currentTerm
		isLeader = false
	} else {
		term = rf.currentTerm
		isLeader = true

		entry := LogEntry{rf.currentTerm, command}
		rf.log = append(rf.log, entry)
		rf.nextIndex[rf.me] += 1

		// fmt.Printf("[%d] start() %v\n", rf.me, command)
		// rf.ShowInfo()
		// fmt.Printf("[%d] %v\n", rf.me, *rf)

		index = len(rf.log) - 1
	}

	return index, term, isLeader
}

func (rf *Raft) logCommit() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// leader commit log after append log to follower
	// fmt.Printf("[%d] %v, %v\n", rf.me, rf.commitIndex, rf.log)
	for commit_id := rf.commitIndex + 1; commit_id < len(rf.log); commit_id++ {
		compilate_cnt := 0
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				compilate_cnt++
				continue
			}

			if rf.isSync[i] && commit_id < rf.nextIndex[i] {
				compilate_cnt++
			}
		}

		// commite
		if compilate_cnt > (len(rf.peers) / 2) {
			rf.commitIndex = commit_id
			// fmt.Println(commit_id, compilate_cnt, (len(rf.peers) / 2))
			// rf.ShowInfo()
			// fmt.Println("[",rf.me,"]", ApplyMsg{true, rf.log[commit_id].Command, commit_id})
			rf.applyCh <- ApplyMsg{true, rf.log[commit_id].Command, commit_id}
		}
	}
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func (rf *Raft) changeToFollower() {
	rf.role = nFollower
	rf.votedFor = -1
	// fmt.Printf("[%d] change to follower\n", rf.me)
}

func (rf *Raft) electionDaemon() {
	for {
		// fmt.Printf("[%d] role: %d\n", rf.me, rf.role)
		rf.mu.Lock()
		role := rf.role
		rf.mu.Unlock()
		switch role {
		case nLeader:
			rf.leaderHandler()
		case nFollower:
			rf.followerHandler()
		case nCandidate:
			rf.candiateHandler()
		}
	}
}

func (rf *Raft) leaderHandler() {
	time.Sleep(10 * time.Millisecond)

	// rf.mu.Lock()
	// rf.counter++
	// fmt.Printf("\n>>>>>>>>>>>>> [%d] %p %d <<<<<<<<<<<<<<<<\n", rf.me, rf, rf.counter)
	// rf.ShowInfo()
	// rf.mu.Unlock()

	for i,_ := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(peer_id int) {
			rf.mu.Lock()
			args := AppendEntriesArgs{
				Term:rf.currentTerm,
				LeaderId:rf.me,

				LeaderCommit:rf.commitIndex,
			}
			reply := AppendEntriesReply{}

			if (rf.nextIndex[peer_id] != rf.nextIndex[rf.me]) {
				args.PrevLogIndex = rf.nextIndex[peer_id]
				args.PrevLogTerm = rf.currentTerm
				// log is empty
				if len(rf.log) == 1 {
					args.PrevLogIndex = 1
					args.PrevLogTerm = rf.currentTerm
				} else {
					// follower's log is emtpy
					if rf.nextIndex[peer_id] == 1 {
						args.PrevLogIndex = 0
						args.PrevLogTerm = 0
						args.Entries = append(args.Entries, rf.log[1:]...)
					} else {
						args.PrevLogIndex = rf.nextIndex[peer_id] - 1
						args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
						args.Entries = append(args.Entries, rf.log[args.PrevLogIndex+1:]...)
					}
				}
			} else {
				if rf.nextIndex[peer_id] > 1 {
					args.PrevLogIndex = rf.nextIndex[peer_id] - 1
					args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
				} else {
					args.PrevLogIndex = 0
					args.PrevLogTerm = 0
				}
			}
			rf.mu.Unlock()

			// fmt.Printf("[%p %d] before %v\n", rf, rf.me, args)
			rf.isSync[peer_id] = false
			ok := rf.sendAppendEntries(peer_id, &args, &reply)
			if ok {
				rf.mu.Lock()
				if !reply.Success {
					if reply.Term > rf.currentTerm {
						rf.changeToFollower()
						rf.currentTerm = reply.Term
					} else {
						if rf.nextIndex[peer_id] > 1 {
							rf.nextIndex[peer_id]--
						}
					}
				} else {
					rf.isSync[peer_id] = true
					if len(args.Entries) > 0 {
						// log append
						rf.nextIndex[peer_id] += len(args.Entries)
						rf.matchIndex[peer_id] += len(args.Entries)
					} else {
						// heartbeat 
						// rf.matchIndex[peer_id] = args.FollowerCommit
					}
				}
				rf.mu.Unlock()
			}
		}(i)
	}

	rf.logCommit()

	// Leader send heartbeat per 0.1s
	time.Sleep(90 * time.Millisecond)
}

func (rf *Raft) followerHandler() {
	// Follower time out check per 1-1.5s
	// time.Sleep(time.Duration((r.Uint32() % 10) * 50 + 1000) * time.Millisecond)

	time.Sleep(100 * time.Millisecond)

	// timeout start election
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.electionTimer += 1;
	if rf.electionTimer > 5 {
		rf.electionTimer = 0
		rf.role = nCandidate
		rf.votedFor = -1
	}
}

func (rf *Raft) candiateHandler() {
	// 1 discover new leader or new term
	// 2 timeout new election
	// 3 receives votes from majority of servers

	rf.mu.Lock()
	me := rf.me
	currentTerm := rf.currentTerm
	rf.mu.Unlock()

	r := rand.New(rand.NewSource(int64((me + 1) * (currentTerm + 1))))
	time.Sleep(time.Duration((r.Uint32() % 10) * 50) * time.Millisecond)
	// fmt.Printf("[%d][%p] %v\n", me, rf, *rf)

	rf.mu.Lock()
	role := rf.role
	rf.mu.Unlock()
	if role != nCandidate {
		return
	}

	rf.mu.Lock()
	rf.currentTerm += 1
	currentTerm = rf.currentTerm
	if rf.votedFor == -1 {
		rf.votedFor = me
	}
	rf.mu.Unlock()

	var counter int32 = 0
	for i,_ := range rf.peers {
		if i == me {
			continue
		}
		go func(peer_id int, counter *int32) {
			args := RequestVoteArgs{
				Term:currentTerm,
				CandidateId:me,
				LastLogIndex:len(rf.log)-1,
				LastLogTerm:rf.log[len(rf.log)-1].Term,
			}
			reply := RequestVoteReply{}

			// fmt.Printf("[%d] vote %v\n", me, args)
			ok := rf.sendRequestVote(peer_id, &args, &reply)
			if ok {
				if reply.Term > rf.currentTerm {
					rf.role = nFollower
				} else {
					if reply.VoteGranted {
						rf.mu.Lock()
						atomic.AddInt32(counter, 1)
						rf.mu.Unlock()
					}
				}
			}
		}(i, &counter)
	}

	// wait for peer's reply
	time.Sleep(20 * time.Millisecond)
	// fmt.Printf("[%d] vote counter %d, majority vote %d\n", me, counter, int32((len(peers)) / 2))
	rf.mu.Lock()
	counter += 1
	// fmt.Printf("[%d] vote counter %d, majority vote %d\n", me, counter, int32((len(rf.peers)) / 2))
	if counter > int32(len(rf.peers) / 2) {
		if rf.role == nCandidate {
			// fmt.Printf("[%d] change to leader\n", me)
			rf.role = nLeader
			rf.votedFor = -1
			rf.isSync[rf.me] = true

			for i := range rf.nextIndex {
				rf.nextIndex[i] = len(rf.log)
				rf.isSync[i] = false
			}
			for i := range rf.matchIndex {
				rf.matchIndex[i] = rf.lastApplied
			}
		} else {
			rf.changeToFollower()
		}
	} else {
		rf.changeToFollower()
	}
	rf.mu.Unlock()
}

func (rf *Raft) candidateSelect() {

}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.votedFor    = -1
	rf.role        = nFollower
	rf.electionTimer = 0
	rf.log = append(rf.log, LogEntry{0, nil})

	rf.isSync = append(rf.isSync, make([]bool, len(rf.peers))...)
	rf.nextIndex = append(rf.nextIndex, make([]int, len(rf.peers))...)
	rf.matchIndex = append(rf.matchIndex, make([]int, len(rf.peers))...)
	for i := 0; i < len(rf.peers); i++ {
		if len(rf.log) > 0 {
			rf.nextIndex[i] = len(rf.log)
		} else {
			rf.nextIndex[i] = 1
		}
	}

	// fmt.Printf("[%d] %p %v\n", rf.me, rf, *rf)

	go rf.electionDaemon()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}
