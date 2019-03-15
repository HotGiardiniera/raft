package main

import (
	"fmt"
	"log"
	"math"
	rand "math/rand"
	"net"
	"time"

	context "golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/nyu-distributed-systems-fa18/lab-2-raft-HotGiardiniera/pb"
)

const LEADER_TIMEOUT = 1000 // Represents the number of milliseconds for a leader heartbeat, Original 500

// Messages that can be passed from the Raft RPC server to the main loop for AppendEntries
type AppendEntriesInput struct {
	arg      *pb.AppendEntriesArgs
	response chan pb.AppendEntriesRet
}

// Messages that can be passed from the Raft RPC server to the main loop for VoteInput
type VoteInput struct {
	arg      *pb.RequestVoteArgs
	response chan pb.RequestVoteRet
}

// Struct off of which we shall hang the Raft service
type Raft struct {
	AppendChan    chan AppendEntriesInput
	VoteChan      chan VoteInput
	InternalState State
}

func (r *Raft) AppendEntries(ctx context.Context, arg *pb.AppendEntriesArgs) (*pb.AppendEntriesRet, error) {
	c := make(chan pb.AppendEntriesRet)
	r.AppendChan <- AppendEntriesInput{arg: arg, response: c}
	result := <-c
	return &result, nil
}

func (r *Raft) RequestVote(ctx context.Context, arg *pb.RequestVoteArgs) (*pb.RequestVoteRet, error) {
	c := make(chan pb.RequestVoteRet)
	r.VoteChan <- VoteInput{arg: arg, response: c}
	result := <-c
	return &result, nil
}

// Compute a random duration in milliseconds
func randomDuration(r *rand.Rand) time.Duration {
	// Constant
	const DurationMax = 6000 // Original 4000
	const DurationMin = 3000 // original 1000
	return time.Duration(r.Intn(DurationMax-DurationMin)+DurationMin) * time.Millisecond
}

// Helper to stop a timer
func stopTimer(timer *time.Timer) {
	if !timer.Stop() {
		for len(timer.C) > 0 {
			<-timer.C
		}
	}
}

// Restart the supplied timer using a random timeout based on function above
func restartTimer(timer *time.Timer, r *rand.Rand) {
	stopTimer(timer)
	timer.Reset(randomDuration(r))
}

// Leader restart
func leaderBeans(timer *time.Timer) {
	// Reference: https://imgur.com/r/thesimpsons/bkTzsJp
	// If stopped is false that means someone stopped before us, which could be due to the timer going off before this,
	// in which case we just drain notifications.
	stopTimer(timer)
	timer.Reset(LEADER_TIMEOUT * time.Millisecond)
}

// Launch a GRPC service for this Raft peer.
func RunRaftServer(r *Raft, port int) {
	// Convert port to a string form
	portString := fmt.Sprintf(":%d", port)
	// Create socket that listens on the supplied port
	c, err := net.Listen("tcp", portString)
	if err != nil {
		// Note the use of Fatalf which will exit the program after reporting the error.
		log.Fatalf("Could not create listening socket %v", err)
	}
	// Create a new GRPC server
	s := grpc.NewServer()

	pb.RegisterRaftServer(s, r)
	log.Printf("Going to listen on port %v", port)

	// Start serving, this will block this function and only return when done.
	if err := s.Serve(c); err != nil {
		log.Fatalf("Failed to serve %v", err)
	}
}

func connectToPeer(peer string) (pb.RaftClient, error) {
	backoffConfig := grpc.DefaultBackoffConfig
	// Choose an aggressive backoff strategy here.
	backoffConfig.MaxDelay = 500 * time.Millisecond
	conn, err := grpc.Dial(peer, grpc.WithInsecure(), grpc.WithBackoffConfig(backoffConfig))
	// Ensure connection did not fail, which should not happen since this happens in the background
	if err != nil {
		return pb.NewRaftClient(nil), err
	}
	return pb.NewRaftClient(conn), nil
}

// ************** Additional conveinece functions **************

type AppendResponse struct {
	ret        *pb.AppendEntriesRet
	err        error
	peer       string
	heartBeat  bool
	matchIndex int64
}

type VoteResponse struct {
	ret  *pb.RequestVoteRet
	err  error
	peer string
}

func DrainVoteChan(voteChan <-chan VoteResponse) {
	for len(voteChan) > 0 {
		<-voteChan
	}
}

func min(x, y int64) int64 {
	if x > y {
		return y
	}
	return x
}
func max(x, y int64) int64 {
	if x > y {
		return x
	}
	return y
}

func (raft *Raft) revertToFollower(
	term int64, timer *time.Timer, voteResponseChan <-chan VoteResponse,
	r *rand.Rand, leader string) {
	redirectClients := (raft.InternalState.serverState == LEADER) && len(raft.InternalState.openClientConns) > 0
	raft.InternalState.currentTerm = term
	raft.InternalState.votedFor = ""
	raft.InternalState.serverState = FOLLOWER
	raft.InternalState.currentLeader = leader

	// DrainVoteChan(voteResponseChan) // At this point we don't need to process any more votes
	if redirectClients { // Redirect any open connections we may have
		for id, client := range raft.InternalState.openClientConns {
			forward_to := raft.InternalState.currentLeader
			client.response <- pb.Result{Result: &pb.Result_Redirect{Redirect: &pb.Redirect{Server: forward_to}}}
			delete(raft.InternalState.openClientConns, id)
		}
	}
	restartTimer(timer, r)
}

func (raft *Raft) sendNOPToPeers(id string, peerClients map[string]pb.RaftClient, appendResponseChan chan<- AppendResponse) {
	// Send out NOP to declare self leader
	log.Printf("Sending NOP to all peers\n")
	// Sending out leader beans
	leaderCurrentTerm := raft.InternalState.currentTerm
	leaderCommitIndex := raft.InternalState.commitIndex
	for p, c := range peerClients {
		peerNextIndex := raft.InternalState.nextIndex[p]
		if peerNextIndex > int64(len(raft.InternalState.log))-1 { // If a command for a peer is higer than our log
			peerNextIndex = int64(len(raft.InternalState.log))
		} else if peerNextIndex < 1 { // all servers should have 0
			peerNextIndex = 1
		}

		// log.Printf("Next term: %v, trying to get: %v Log len %v", peerNextIndex, peerNextIndex-1, len(raft.InternalState.log))
		peerPrevLogTerm := raft.InternalState.log[peerNextIndex-1].Term
		go func(c pb.RaftClient, p string, currentTerm, newPeerIndex, prevLogTerm, commitIndex int64) {
			ret, err := c.AppendEntries(context.Background(), &pb.AppendEntriesArgs{
				Term:         currentTerm,
				LeaderID:     id,
				PrevLogIndex: newPeerIndex - 1,
				PrevLogTerm:  prevLogTerm,
				LeaderCommit: commitIndex})
			appendResponseChan <- AppendResponse{ret: ret, err: err, peer: p, heartBeat: true}
		}(c, p, leaderCurrentTerm, peerNextIndex, peerPrevLogTerm, leaderCommitIndex)
	}
	log.Printf("Leader %v\nCurrent log %v", id, raft.logString())
}

func (raft *Raft) initializePeerIndexes(peers map[string]pb.RaftClient) {
	last_log_index := int64(len(raft.InternalState.log))
	for peer, _ := range peers {
		raft.InternalState.nextIndex[peer] = last_log_index
		raft.InternalState.matchIndex[peer] = 0
	}
}

func (raft *Raft) logString() string {
	retString := "["
	for index, entry := range raft.InternalState.log {
		retString += fmt.Sprintf("%v:%v", entry.Index, entry.Term)
		if index != len(raft.InternalState.log)-1 {
			retString += ", "
		}
	}
	return retString + "]"
}

// *************************************************************

// The main service loop. All modifications to the KV store are run through here.
func serve(s *KVStore, r *rand.Rand, peers *arrayPeers, id string, port int) {
	raft := Raft{AppendChan: make(chan AppendEntriesInput), VoteChan: make(chan VoteInput),
		InternalState: State{serverState: FOLLOWER,
			log:             []*pb.Entry{&pb.Entry{Term: 0, Index: 0, Cmd: &pb.Command{}}},
			nextIndex:       make(peerNextIndex),
			matchIndex:      make(peerMatchIndex),
			openClientConns: make(map[string]*InputChannelType)}}
	//log.Printf("WHY DON'T YOU HAVE ANYTHING IN THE LOG %v length %v", raft.InternalState.log, len(raft.InternalState.log))
	// Start in a Go routine so it doesn't affect us.
	go RunRaftServer(&raft, port)

	peerClients := make(map[string]pb.RaftClient)

	for _, peer := range *peers {
		client, err := connectToPeer(peer)
		if err != nil {
			log.Fatalf("Failed to connect to GRPC server %v", err)
		}

		peerClients[peer] = client
		raft.InternalState.nextIndex[peer] = int64(len(raft.InternalState.log))
		log.Printf("Connected to %v", peer)
	}

	appendResponseChan := make(chan AppendResponse)
	voteResponseChan := make(chan VoteResponse)
	// fakeKVResponseChan := make(chan pb.Result) // Channel for fastforwarding only

	// Create a timer and start running it
	timer := time.NewTimer(randomDuration(r))

	// Run forever handling inputs from various channels
	for {
		select {
		case <-timer.C:
			// Check if we are the leader and send out NOPs
			if raft.InternalState.serverState == LEADER {
				raft.sendNOPToPeers(id, peerClients, appendResponseChan)
				leaderBeans(timer)
			} else {
				// The timer went off. update term and vote for self
				raft.InternalState.serverState = CANIDATE
				raft.InternalState.currentTerm++
				raft.InternalState.votedFor = id // Self vote
				raft.InternalState.voteCount = 1
				log.Printf("\u001b[35mTimeout, New term: %v\n\u001b[0m", raft.InternalState.currentTerm)

				lastIndex := int64(len(raft.InternalState.log)) - 1
				myCurrentTerm := raft.InternalState.currentTerm
				myLastLogTerm := raft.InternalState.log[lastIndex].Term
				for p, c := range peerClients {
					// Send in parallel so we don't wait for each client.
					go func(c pb.RaftClient, p string, currentTerm, lastLogTerm int64) {
						ret, err := c.RequestVote(context.Background(),
							&pb.RequestVoteArgs{
								Term:         currentTerm,
								CandidateID:  id,
								LastLogIndex: lastIndex,
								LasLogTerm:   lastLogTerm})
						voteResponseChan <- VoteResponse{ret: ret, err: err, peer: p}
					}(c, p, myCurrentTerm, myLastLogTerm)
				}
				// This will also take care of any pesky timeouts that happened while processing the operation.
				restartTimer(timer, r)
			}
		case op := <-s.C:
			// We received an operation from a client
			// log.Printf("\u001b[33mReceived a client request %v\u001b[0m", op)
			if raft.InternalState.serverState == LEADER {
				new_log_index := int64(len(raft.InternalState.log))
				new_entry := &pb.Entry{Cmd: &op.command, Term: raft.InternalState.currentTerm, Index: new_log_index}
				raft.InternalState.log = append(raft.InternalState.log, new_entry)
				// log.Printf("\u001b[33mLeader's current log %v New log index %v My Commit Index: %v \u001b[0m",
				// raft.logString(), new_log_index, raft.InternalState.commitIndex)
				// Add the operation to our open clients map
				indexTermString := fmt.Sprintf("%v:%v", new_log_index, raft.InternalState.currentTerm)
				raft.InternalState.openClientConns[indexTermString] = &op

				// Variables needed to pass into the go routine so they are npt changed while a routine is processins
				leaderCommitIndex := raft.InternalState.commitIndex
				myCurrentTerm := raft.InternalState.currentTerm
				for p, c := range peerClients {
					peerNextIndex := raft.InternalState.nextIndex[p]
					if peerNextIndex > int64(len(raft.InternalState.log))-1 { // If a command for a peer is higer than our log
						peerNextIndex = int64(len(raft.InternalState.log))
					} else if peerNextIndex < 1 { // all servers should have 0
						peerNextIndex = 1
					}
					peerPrevLogTerm := raft.InternalState.log[peerNextIndex-1].Term
					entriesToSend := raft.InternalState.log[peerNextIndex:]
					// Send in parallel so we don't wait for each client.
					go func(c pb.RaftClient, p string, newEntries []*pb.Entry,
						currentTerm, newPeerIndex, prevLogTerm, commitIndex int64) {
						// log.Printf("\u001b[33mPeer index I have: %v Sending %v\u001b[0m", newPeerIndex, newEntries)
						ret, err := c.AppendEntries(context.Background(),
							&pb.AppendEntriesArgs{
								Term:         myCurrentTerm,
								LeaderID:     id,
								PrevLogIndex: newPeerIndex - 1,
								PrevLogTerm:  prevLogTerm,
								Entries:      newEntries,
								LeaderCommit: commitIndex})
						newMatchIndex := (newPeerIndex - 1) + int64(len(newEntries)) // -1  ??
						appendResponseChan <- AppendResponse{ret: ret, err: err, peer: p, heartBeat: false, matchIndex: newMatchIndex}
					}(c, p, entriesToSend, myCurrentTerm, peerNextIndex, peerPrevLogTerm, leaderCommitIndex)
				}
				leaderBeans(timer)
			} else { // Redirect to client with correct server info TODO
				forward_to := raft.InternalState.currentLeader
				if forward_to == "" {
					forward_to = id
				}
				op.response <- pb.Result{Result: &pb.Result_Redirect{Redirect: &pb.Redirect{Server: forward_to}}}
			}

		case ae := <-raft.AppendChan:
			// We received an AppendEntries request from a Raft peer
			// log.Printf("\u001b[36mReceived append entry from --> %v\u001b[0m ", ae.arg.LeaderID)

			// First if we get a heartbeat form a leader update our term and revert to follower
			if ae.arg.Term >= raft.InternalState.currentTerm {
				raft.revertToFollower(ae.arg.Term, timer, voteResponseChan, r, ae.arg.LeaderID)

				// Check If I can append the entry, i.e log is up to date with leader
				switch {

				case ae.arg.PrevLogIndex > int64(len(raft.InternalState.log))-1 ||
					raft.InternalState.log[ae.arg.PrevLogIndex].Term != ae.arg.PrevLogTerm:
					// log.Printf("\u001b[31m     Replying False to append entries Prev Log %v, my Log last %v\u001b[0m", ae.arg.PrevLogIndex, int64(len(raft.InternalState.log))-1)
					ae.response <- pb.AppendEntriesRet{Term: raft.InternalState.currentTerm, Success: false}
				default:
					if len(ae.arg.Entries) > 0 {
						//log.Printf("\u001b[36mAPPLYING CHANGES My max index: %v  response Arg index: %v\u001b[0m ", int64(len(raft.InternalState.log))-1, ae.arg.PrevLogIndex)
						// Overwrite if we have a conflicting append, same index but different term
						lastLogIndex := int64(len(raft.InternalState.log) - 1)
						if ae.arg.PrevLogIndex <= lastLogIndex && raft.InternalState.log[ae.arg.PrevLogIndex].Term != ae.arg.PrevLogTerm {
							raft.InternalState.log = raft.InternalState.log[:ae.arg.PrevLogIndex+1] // overwrite any stuff I may have
							lastLogIndex = int64(len(raft.InternalState.log) - 1)
							log.Printf("\u001b[33mOverwriting log, up to %v. New log %v\u001b[0m", ae.arg.PrevLogIndex, raft.InternalState.log)
						}

						for i, entry := range ae.arg.Entries {
							index := entry.Index
							if index > lastLogIndex {
								raft.InternalState.log = append(raft.InternalState.log, ae.arg.Entries[i:]...)
								break
							}
						}
					}

					// We can apply if the commit index is above lastApplies
					if ae.arg.LeaderCommit > raft.InternalState.commitIndex {
						raft.InternalState.commitIndex = min(ae.arg.LeaderCommit, int64(len(raft.InternalState.log)-1))
						if raft.InternalState.commitIndex > raft.InternalState.lastApplied {
							for raft.InternalState.lastApplied < raft.InternalState.commitIndex {
								raft.InternalState.lastApplied++
								cmd := raft.InternalState.log[raft.InternalState.lastApplied].Cmd
								log.Printf(" \u001b[35mWARNING ABOUT TO APPLY COMMAND TO STATE MACHINE %v INDEX: %v\u001b[0m", cmd, raft.InternalState.lastApplied)
								newOp := InputChannelType{command: *cmd, response: make(chan pb.Result, 1)}
								s.HandleCommand(newOp)
								<-newOp.response
								log.Printf(" \u001b[35mWARNING COMMAND  APPLIED TO STATE MACHINE %v INDEX: %v\u001b[0m", cmd, raft.InternalState.lastApplied)
							}
						}
					}

					ae.response <- pb.AppendEntriesRet{Term: raft.InternalState.currentTerm, Success: true}
					// This will also take care of any pesky timeouts that happened while processing the operation.
					restartTimer(timer, r)
				}
			} else {
				ae.response <- pb.AppendEntriesRet{Term: raft.InternalState.currentTerm, Success: false}
			}
			log.Printf("Peer %v\nCurrent log %v", id, raft.logString())
		case vr := <-raft.VoteChan:
			// We received a RequestVote RPC from a raft peer

			// Defualt pessemistic vote
			vote := false
			term := raft.InternalState.currentTerm
			// Only vote positively if my current term is less than or equal to the voting term
			// and I have not voted for someone, or already voted for the requester
			if term < vr.arg.Term {
				raft.revertToFollower(vr.arg.Term, timer, voteResponseChan, r, id)
			}
			if raft.InternalState.currentTerm == vr.arg.Term {
				last_log_index := int64(len(raft.InternalState.log)) - 1
				last_log_term := raft.InternalState.log[last_log_index].Term
				if (raft.InternalState.votedFor == "" || raft.InternalState.votedFor == vr.arg.CandidateID) &&
					((vr.arg.LasLogTerm > last_log_term) || ((last_log_term == vr.arg.LasLogTerm) && vr.arg.LastLogIndex >= last_log_index)) {
					vote = true
					term = vr.arg.Term
					raft.InternalState.currentTerm = term
					raft.InternalState.votedFor = vr.arg.CandidateID
					restartTimer(timer, r)
				}
			}
			log.Printf("Received vote request from --> %v for term %v voting %v", vr.arg.CandidateID, vr.arg.Term, vote)
			vr.response <- pb.RequestVoteRet{Term: term, VoteGranted: vote}

		case vr := <-voteResponseChan:
			// We received a response to a previous vote request.
			if vr.err != nil {
				// Do not do Fatalf here since the peer might be gone but we should survive.
				// log.Printf("Error calling RPC %v", vr.err)
			} else {
				log.Printf("Got response to vote request from --> %v", vr.peer)
				log.Printf("Peers %s granted %v term %v", vr.peer, vr.ret.VoteGranted, vr.ret.Term)
				// Check if we got a positive response, if negative it is possible that our term is behind
				// increment a term vote postive count,
				// if positive vote count > num_peers/2
				if raft.InternalState.currentTerm < vr.ret.Term {
					// Term is off revert to follower state
					raft.revertToFollower(vr.ret.Term, timer, voteResponseChan, r, id)
				} else if vr.ret.VoteGranted && raft.InternalState.serverState == CANIDATE { // Vote granted
					raft.InternalState.voteCount++ // handled by the fact that we set vote count to 1 when we start an election
					if raft.InternalState.voteCount > int64(math.Ceil(float64(len(*peers))/2)) {
						log.Printf("\u001b[32m%v became leader!! term: %v \u001b[0m\n", id, raft.InternalState.currentTerm)
						raft.InternalState.serverState = LEADER
						raft.InternalState.currentLeader = id
						raft.InternalState.voteCount = 0
						raft.initializePeerIndexes(peerClients)
						// DrainVoteChan(voteResponseChan)
						raft.sendNOPToPeers(id, peerClients, appendResponseChan)
						leaderBeans(timer)
					}
				}

			}
		case ar := <-appendResponseChan:
			// We received a response to a previous AppendEntries RPC call
			// log.Printf("Peer %v current log %v", id, raft.InternalState.log)
			if ar.err != nil {
				if !ar.heartBeat && raft.InternalState.serverState == LEADER { // retry a client request indefinitely
					new_peer_index := raft.InternalState.nextIndex[ar.peer]
					if new_peer_index > int64(len(raft.InternalState.log))-1 {
						new_peer_index = int64(len(raft.InternalState.log)) - 1
					} else if new_peer_index < 0 {
						new_peer_index = 0
					}
					new_term := raft.InternalState.log[new_peer_index].Term
					entries := raft.InternalState.log[new_peer_index:]
					leaderCommitIndex := raft.InternalState.commitIndex
					myCurrentTerm := raft.InternalState.currentTerm

					go func(newIndex, newTerm, commitIndex, leaderTerm int64, entries []*pb.Entry) {
						ret, err := peerClients[ar.peer].AppendEntries(context.Background(), &pb.AppendEntriesArgs{
							Term:         leaderTerm,
							LeaderID:     id,
							PrevLogIndex: newIndex,
							PrevLogTerm:  newTerm,
							Entries:      entries,
							LeaderCommit: commitIndex})
						// new_match_index := new_peer_index + int64(len(entries)) - 1
						appendResponseChan <- AppendResponse{ret: ret, err: err, peer: ar.peer, heartBeat: false, matchIndex: ar.matchIndex}
					}(new_peer_index, new_term, leaderCommitIndex, myCurrentTerm, entries)
				}
				//log.Printf("Peer %v current log %v", id, raft.InternalState.log)
			} else {
				// log.Printf("Got append entries response from %v, arr err: %v", ar.peer, ar.err)
				switch {
				case ar.ret.Term > raft.InternalState.currentTerm: // revert to follower if leader
					raft.revertToFollower(ar.ret.Term, timer, voteResponseChan, r, id)

				case ar.ret.Term < raft.InternalState.currentTerm: // This prevents unecessary applies
					// log.Printf("IGNORING OLD TERM RESPONSE")

				// case ar.heartBeat:
				// log.Printf("\u001b[36;1mheartbeat\u001b[0m response from follower")

				case ar.ret.Success:
					if raft.InternalState.serverState == LEADER {
						raft.InternalState.matchIndex[ar.peer] = max(ar.matchIndex, raft.InternalState.matchIndex[ar.peer])
						raft.InternalState.nextIndex[ar.peer] = raft.InternalState.matchIndex[ar.peer] + 1
						// log.Printf("\u001b[32m     Successful reply on append entries %v next index %v match index %v\u001b[0m", ar.peer, raft.InternalState.nextIndex[ar.peer], raft.InternalState.matchIndex[ar.peer])
						// Check rule for ability to commit index we want to do this in our thread
						// First find the first index of a Log entry in this term
						var repcount int64
						for i := int64(len(raft.InternalState.log) - 1); i > 0; i-- {
							if raft.InternalState.log[i].Term < raft.InternalState.currentTerm {
								break
							}
							repcount = 1
							for _, index := range raft.InternalState.matchIndex {
								if index >= i {
									repcount++
								}
							}
							if (repcount > int64(math.Ceil(float64(len(*peers))/2))) && (i > raft.InternalState.commitIndex) {
								raft.InternalState.commitIndex = i
								log.Printf("\u001b[32m     Able to confidently update the commit index, Votes: %v commitIndex: %v Term: %v\u001b[0m",
									repcount, i, raft.InternalState.currentTerm)
								break
							}
						}
						if raft.InternalState.commitIndex > raft.InternalState.lastApplied {
							for raft.InternalState.lastApplied < raft.InternalState.commitIndex {
								raft.InternalState.lastApplied++
								cmd := raft.InternalState.log[raft.InternalState.lastApplied].Cmd
								log.Printf(" \u001b[35mWARNING ABOUT TO APPLY COMMAND TO STATE MACHINE %v INDEX: %v\u001b[0m", cmd, raft.InternalState.lastApplied)
								openConnectionString := fmt.Sprintf("%v:%v", raft.InternalState.log[raft.InternalState.lastApplied].Index,
									raft.InternalState.log[raft.InternalState.lastApplied].Term)
								if op, ok := raft.InternalState.openClientConns[openConnectionString]; ok {
									s.HandleCommand(*op)
									delete(raft.InternalState.openClientConns, openConnectionString)
								} else {
									newOp := InputChannelType{command: *cmd, response: make(chan pb.Result, 1)}
									s.HandleCommand(newOp)
									<-newOp.response
								}
							}
						}
					}
					// Do not commit to state machine unless something is commited from your current term
					// Check if there is an index where the term == current term and that index is replicated on a majority of the servers

				case ar.ret.Success == false:
					if raft.InternalState.serverState == LEADER {
						log.Printf("\u001b[31m     False reply on append entries \u001b[0m")
						if raft.InternalState.nextIndex[ar.peer] > 0 { // 0 is always in common
							raft.InternalState.nextIndex[ar.peer]--
						}
						new_peer_index := raft.InternalState.nextIndex[ar.peer]
						if new_peer_index > int64(len(raft.InternalState.log))-1 {
							new_peer_index = int64(len(raft.InternalState.log)) - 1
						}
						new_term := raft.InternalState.log[new_peer_index].Term

						// Send over as many entries as we can
						entries := raft.InternalState.log[new_peer_index:] //TEST!!!!!!
						// log.Printf("\u001b[31m     False reply on append entries %v Retrying... new index %v\n\t\t Retrying with entries %v\u001b[0m",
						// ar.peer, new_peer_index, entries)

						leaderCommitIndex := raft.InternalState.commitIndex
						myCurrentTerm := raft.InternalState.currentTerm
						go func(newIndex, newTerm, commitIndex, leaderTerm int64, entries []*pb.Entry) {
							ret, err := peerClients[ar.peer].AppendEntries(context.Background(), &pb.AppendEntriesArgs{
								Term:         leaderTerm,
								LeaderID:     id,
								PrevLogIndex: newIndex,
								PrevLogTerm:  newTerm,
								Entries:      entries,
								LeaderCommit: commitIndex})
							new_match_index := newIndex + int64(len(entries)) - 1
							appendResponseChan <- AppendResponse{ret: ret, err: err, peer: ar.peer, heartBeat: false, matchIndex: new_match_index}
						}(new_peer_index, new_term, leaderCommitIndex, myCurrentTerm, entries)
					}
				}
			}
		} // End switch
	} // End for
	// log.Printf("Strange to arrive here") // `go vet` complains about this...
}
