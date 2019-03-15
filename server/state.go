// Note: This file was created in collaboration with Yair Schiff
package main

import (
	"github.com/nyu-distributed-systems-fa18/lab-2-raft-HotGiardiniera/pb"
)

type peerNextIndex map[string]int64
type peerMatchIndex map[string]int64

type ServerState int64

const (
	FOLLOWER ServerState = 1
	CANIDATE ServerState = 2
	LEADER   ServerState = 3
)

// type LogEntry struct {
// 	command *pb.Command
// 	term    int64
// 	//index   int64 // Probably not needed good for debug
// 	replication_count int64
// }

type State struct { // holds server state variables
	currentTerm int64
	votedFor    string
	serverState ServerState
	voteCount   int64

	// Volatile state variables
	log         []*pb.Entry // Everyone's log
	commitIndex int64
	lastApplied int64

	// Leader variables. Should be reitialized after each election.
	nextIndex  peerNextIndex
	matchIndex peerMatchIndex

	// Debug states
	currentLeader string
	//Map of open client connections I have
	openClientConns map[string]*InputChannelType
}
