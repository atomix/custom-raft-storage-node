// Copyright 2019-present Open Networking Foundation.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package roles

import (
	"container/list"
	"context"
	"errors"
	raft "github.com/atomix/atomix-raft-node/pkg/atomix/raft/protocol"
	"github.com/atomix/atomix-raft-node/pkg/atomix/raft/state"
	"github.com/atomix/atomix-raft-node/pkg/atomix/raft/store"
	"github.com/atomix/atomix-raft-node/pkg/atomix/raft/store/log"
	"github.com/atomix/atomix-raft-node/pkg/atomix/raft/store/snapshot"
	"github.com/atomix/atomix-raft-node/pkg/atomix/raft/util"
	"math"
	"sort"
	"sync"
	"time"
)

// newAppender returns a new appender
func newAppender(state raft.Raft, sm state.Manager, store store.Store, log util.Logger) *raftAppender {
	commitCh := make(chan memberCommit)
	failCh := make(chan time.Time)
	members := make(map[raft.MemberID]*memberAppender)
	for _, memberID := range state.Members() {
		if memberID != state.Member() {
			members[memberID] = newMemberAppender(state, sm, store, log, state.GetMember(memberID), commitCh, failCh)
		}
	}
	appender := &raftAppender{
		raft:             state,
		sm:               sm,
		store:            store,
		log:              log,
		members:          members,
		commitIndexes:    make(map[raft.MemberID]raft.Index),
		commitTimes:      make(map[raft.MemberID]time.Time),
		heartbeatFutures: list.New(),
		commitChannels:   make(map[raft.Index]chan bool),
		commitFutures:    make(map[raft.Index]func()),
		commitCh:         commitCh,
		failCh:           failCh,
		lastQuorumTime:   time.Now(),
		stopped:          make(chan bool),
	}
	return appender
}

// raftAppender handles replication on the leader
type raftAppender struct {
	raft             raft.Raft
	sm               state.Manager
	store            store.Store
	log              util.Logger
	members          map[raft.MemberID]*memberAppender
	commitIndexes    map[raft.MemberID]raft.Index
	commitTimes      map[raft.MemberID]time.Time
	heartbeatFutures *list.List
	commitChannels   map[raft.Index]chan bool
	commitFutures    map[raft.Index]func()
	commitCh         chan memberCommit
	failCh           chan time.Time
	stopped          chan bool
	lastQuorumTime   time.Time
	mu               sync.Mutex
}

// start starts the appender
func (a *raftAppender) start() {
	for _, member := range a.members {
		go member.start()
	}
	a.processCommits()
}

// heartbeat sends a heartbeat to a majority of followers
func (a *raftAppender) heartbeat() error {
	// If there are no members to send the entry to, immediately return.
	if len(a.members) == 0 {
		return nil
	}

	future := heartbeatFuture{}

	// Acquire a lock to add the future to the heartbeat futures.
	a.mu.Lock()
	a.heartbeatFutures.PushBack(future)
	a.mu.Unlock()

	// Iterate through member appenders and add the future time to the heartbeat channels.
	for _, member := range a.members {
		member.heartbeatCh <- future.time
	}
	_, ok := <-future.ch
	if ok {
		return nil
	}
	return errors.New("failed to verify quorum")
}

// commit replicates the given entry to followers and returns once the entry is committed
func (a *raftAppender) commit(entry *log.Entry, f func()) error {
	// If there are no members to send the entry to, immediately commit it.
	if len(a.members) == 0 {
		a.raft.WriteLock()
		a.raft.SetCommitIndex(entry.Index)
		a.raft.Commit(entry.Index)
		if f != nil {
			f()
		}
		a.raft.WriteUnlock()
		return nil
	}

	// Acquire a write lock on the appender and add the channel to commitFutures.
	a.mu.Lock()
	ch := make(chan bool)
	a.commitChannels[entry.Index] = ch
	if f != nil {
		a.commitFutures[entry.Index] = f
	}
	a.mu.Unlock()

	// Push the entry onto the channel for each member appender
	for _, member := range a.members {
		member.entryCh <- entry
	}

	// Wait for the commit channel.
	succeeded, ok := <-ch
	if ok && succeeded {
		return nil
	}
	return errors.New("failed to commit entry")
}

// processCommits handles member commit events and updates the local commit index
func (a *raftAppender) processCommits() {
	for {
		select {
		case commit := <-a.commitCh:
			a.commitMember(commit.member, commit.index, commit.time)
		case failTime := <-a.failCh:
			a.failTime(failTime)
		case <-a.stopped:
			return
		}
	}
}

func (a *raftAppender) commitMember(member *memberAppender, index raft.Index, time time.Time) {
	if !member.active {
		return
	}
	a.commitMemberIndex(member.member.MemberID, index)
	a.commitMemberTime(member.member.MemberID, time)
}

func (a *raftAppender) commitMemberIndex(member raft.MemberID, index raft.Index) {
	prevIndex := a.commitIndexes[member]
	if index > prevIndex {
		a.commitIndexes[member] = index

		indexes := make([]raft.Index, len(a.members))
		i := 0
		for _, index := range a.commitIndexes {
			indexes[i] = index
			i++
		}
		sort.Slice(indexes, func(i, j int) bool {
			return indexes[i] < indexes[j]
		})

		commitIndex := indexes[len(a.members)/2]
		a.raft.ReadLock()
		if commitIndex > a.raft.CommitIndex() {
			a.raft.ReadUnlock()
			a.raft.WriteLock()
			if commitIndex > a.raft.CommitIndex() {
				for i := a.raft.CommitIndex() + 1; i <= commitIndex; i++ {
					a.commitIndex(i)
				}
				a.raft.WriteUnlock()
				a.log.Trace("Committed entries up to %d", commitIndex)
			} else {
				a.raft.WriteUnlock()
			}
		} else {
			a.raft.ReadUnlock()
		}
	}
}

func (a *raftAppender) commitIndex(index raft.Index) {
	// Update the server commit index.
	a.raft.SetCommitIndex(index)
	a.raft.Commit(index)

	// Acquire a lock on the appender and complete the commit channels and futures.
	a.mu.Lock()
	ch, ok := a.commitChannels[index]
	if ok {
		ch <- true
		delete(a.commitChannels, index)
	}
	f, ok := a.commitFutures[index]
	if ok {
		f()
		delete(a.commitFutures, index)
	}
	a.mu.Unlock()
}

func (a *raftAppender) commitMemberTime(member raft.MemberID, time time.Time) {
	prevTime := a.commitTimes[member]
	nextTime := time
	if nextTime.UnixNano() > prevTime.UnixNano() {
		a.commitTimes[member] = nextTime

		times := make([]int64, len(a.members))
		i := 0
		for _, t := range a.commitTimes {
			times[i] = t.UnixNano()
			i++
		}
		sort.Slice(times, func(i, j int) bool {
			return times[i] < times[j]
		})

		commitTime := times[len(a.members)/2]
		a.mu.Lock()
		for commitFuture := a.heartbeatFutures.Front(); commitFuture != nil && commitFuture.Value.(heartbeatFuture).time.UnixNano() < commitTime; commitFuture = a.heartbeatFutures.Front() {
			ch := commitFuture.Value.(heartbeatFuture).ch
			ch <- struct{}{}
			close(ch)
			a.heartbeatFutures.Remove(commitFuture)
		}
		a.mu.Unlock()

		// Update the last time a quorum of the cluster was reached
		a.lastQuorumTime = time
	}
}

func (a *raftAppender) failTime(failTime time.Time) {
	if failTime.Sub(a.lastQuorumTime) > a.raft.Config().GetElectionTimeoutOrDefault()*2 {
		a.log.Warn("Suspected network partition; stepping down")
		_ = a.raft.SetLeader(nil)
		go a.raft.SetRole(newFollowerRole(a.raft, a.sm, a.store))
	}
}

func (a *raftAppender) stop() {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, member := range a.members {
		member.stop()
	}
	a.stopped <- true
}

// heartbeatFuture is a heartbeat channel with a timestamp indicating when the heartbeat was requested
type heartbeatFuture struct {
	ch   chan struct{}
	time time.Time
}

// memberCommit is an event carrying the match index for a member
type memberCommit struct {
	member *memberAppender
	index  raft.Index
	time   time.Time
}

const (
	minBackoffFailureCount = 5
	maxHeartbeatWait       = 1 * time.Minute
	maxBatchSize           = 1024 * 1024
)

func newMemberAppender(state raft.Raft, sm state.Manager, store store.Store, logger util.Logger, member *raft.RaftMember, commitCh chan<- memberCommit, failCh chan<- time.Time) *memberAppender {
	ticker := time.NewTicker(state.Config().GetElectionTimeoutOrDefault() / 2)
	reader := store.Log().OpenReader(0)
	return &memberAppender{
		raft:        state,
		sm:          sm,
		store:       store,
		log:         logger,
		member:      member,
		nextIndex:   reader.LastIndex() + 1,
		entryCh:     make(chan *log.Entry),
		appendCh:    make(chan bool),
		commitCh:    commitCh,
		failCh:      failCh,
		heartbeatCh: make(chan time.Time),
		stopped:     make(chan bool),
		reader:      reader,
		tickTicker:  ticker,
		tickCh:      ticker.C,
		queue:       list.New(),
	}
}

// memberAppender handles replication to a member
type memberAppender struct {
	raft             raft.Raft
	sm               state.Manager
	store            store.Store
	log              util.Logger
	member           *raft.RaftMember
	active           bool
	snapshotIndex    raft.Index
	prevTerm         raft.Term
	nextIndex        raft.Index
	matchIndex       raft.Index
	appending        bool
	failureCount     int
	firstFailureTime time.Time
	entryCh          chan *log.Entry
	appendCh         chan bool
	commitCh         chan<- memberCommit
	failCh           chan<- time.Time
	heartbeatCh      chan time.Time
	tickCh           <-chan time.Time
	tickTicker       *time.Ticker
	stopped          chan bool
	reader           log.Reader
	queue            *list.List
	mu               sync.Mutex
}

// start starts sending append requests to the member
func (a *memberAppender) start() {
	a.active = true
	a.processEvents()
}

func (a *memberAppender) processEvents() {
	for {
		select {
		case entry := <-a.entryCh:
			if a.failureCount == 0 {
				a.mu.Lock()
				a.queue.PushBack(entry)
				a.mu.Unlock()
			}
			if !a.appending {
				a.appending = true
				go a.append()
			}
		case hasEntries := <-a.appendCh:
			a.appending = false
			if hasEntries {
				a.appending = true
				go a.append()
			}
		case <-a.heartbeatCh:
			go a.sendAppendRequest(a.emptyAppendRequest())
		case <-a.tickCh:
			if !a.appending {
				a.appending = true
				go a.append()
			}
		case <-a.stopped:
			return
		}
	}
}

func (a *memberAppender) append() {
	if a.failureCount >= minBackoffFailureCount {
		timeSinceFailure := float64(time.Since(a.firstFailureTime))
		heartbeatWaitTime := math.Min(float64(a.failureCount)*float64(a.failureCount)*float64(a.raft.Config().GetElectionTimeoutOrDefault()), float64(maxHeartbeatWait))
		if timeSinceFailure > heartbeatWaitTime {
			a.sendAppendRequest(a.nextAppendRequest())
		}
	} else {
		// TODO: The snapshot store needs concurrency control when accessing the snapshots for replication.
		snapshot := a.store.Snapshot().CurrentSnapshot()
		if snapshot != nil && a.snapshotIndex < snapshot.Index() && snapshot.Index() >= a.nextIndex {
			a.log.Debug("Replicating snapshot %d to %s", snapshot.Index(), a.member.MemberID)
			a.sendInstallRequests(snapshot)
		} else {
			a.sendAppendRequest(a.nextAppendRequest())
		}
	}
}

// stop stops sending append requests to the member
func (a *memberAppender) stop() {
	a.active = false
	a.tickTicker.Stop()
	a.stopped <- true
}

func (a *memberAppender) succeed() {
	a.failureCount = 0
}

func (a *memberAppender) fail(time time.Time) {
	if a.failureCount == 0 {
		a.firstFailureTime = time
	}
	a.failureCount++
	a.failCh <- time
}

func (a *memberAppender) requeue() {
	a.raft.ReadLock()
	hasEntries := a.reader.LastIndex() >= a.nextIndex
	a.raft.ReadUnlock()
	a.appendCh <- hasEntries
}

func (a *memberAppender) newInstallRequest(snapshot snapshot.Snapshot, bytes []byte) *raft.InstallRequest {
	a.raft.ReadLock()
	defer a.raft.ReadUnlock()
	return &raft.InstallRequest{
		Term:      a.raft.Term(),
		Leader:    a.raft.Member(),
		Index:     snapshot.Index(),
		Timestamp: snapshot.Timestamp(),
		Data:      bytes,
	}
}

func (a *memberAppender) sendInstallRequests(snapshot snapshot.Snapshot) {
	// Start the append to the member.
	startTime := time.Now()

	client, err := a.raft.Connect(a.member.MemberID)
	if err != nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), a.raft.Config().GetElectionTimeoutOrDefault())
	defer cancel()

	stream, err := client.Install(ctx)
	if err != nil {
		a.handleInstallError(snapshot, err, startTime)
		return
	}

	reader := snapshot.Reader()
	defer func() {
		_ = reader.Close()
	}()
	bytes := make([]byte, maxBatchSize)
	n, err := reader.Read(bytes)
	for n > 0 && err == nil {
		request := a.newInstallRequest(snapshot, bytes[:n])
		a.log.SendTo("InstallRequest", request, a.member.MemberID)
		_ = stream.Send(request)
		n, err = reader.Read(bytes)
	}
	if err != nil {
		a.log.Warn("Failed to read snapshot", err)
	}

	response, err := stream.CloseAndRecv()
	if err == nil {
		a.log.ReceiveFrom("InstallResponse", response, a.member.MemberID)
		if response.Status == raft.ResponseStatus_OK {
			a.handleInstallResponse(snapshot, response, startTime)
		} else {
			a.handleInstallFailure(snapshot, response, startTime)
		}
	} else {
		a.log.ErrorFrom("InstallRequest", err, a.member.MemberID)
		a.handleInstallError(snapshot, err, startTime)
	}
}

func (a *memberAppender) handleInstallResponse(snapshot snapshot.Snapshot, response *raft.InstallResponse, startTime time.Time) {
	// Reset the member failure count to allow entries to be sent to the member.
	a.succeed()

	// Update the snapshot index
	a.snapshotIndex = snapshot.Index()

	// Send a commit event to the parent appender.
	a.commit(startTime)

	// Requeue the append for the nextIndex.
	a.requeue()
}

func (a *memberAppender) handleInstallFailure(snapshot snapshot.Snapshot, response *raft.InstallResponse, startTime time.Time) {
	// In the event of an install response error, simply do nothing and await the next heartbeat.
	// This prevents infinite loops when installation fails.
}

func (a *memberAppender) handleInstallError(snapshot snapshot.Snapshot, err error, startTime time.Time) {
	a.log.Debug("Failed to install %s: %s", a.member.MemberID, err)
	a.fail(startTime)
	a.requeue()
}

func (a *memberAppender) nextAppendRequest() *raft.AppendRequest {
	// If the log is empty then send an empty commit.
	// If the next index hasn't yet been set then we send an empty commit first.
	// If the next index is greater than the last index then send an empty commit.
	// If the member failed to respond to recent communication send an empty commit. This
	// helps avoid doing expensive work until we can ascertain the member is back up.
	a.raft.ReadLock()
	defer a.raft.ReadUnlock()
	if a.failureCount > 0 || a.reader.CurrentIndex() == a.reader.LastIndex() {
		return a.emptyAppendRequest()
	}
	return a.entriesAppendRequest()
}

func (a *memberAppender) emptyAppendRequest() *raft.AppendRequest {
	prevIndex := a.nextIndex - 1
	if a.prevTerm == 0 && prevIndex >= a.reader.FirstIndex() {
		a.reader.Reset(prevIndex)
		a.prevTerm = a.reader.NextEntry().Entry.Term
	}
	return &raft.AppendRequest{
		Term:         a.raft.Term(),
		Leader:       a.raft.Member(),
		PrevLogIndex: a.nextIndex - 1,
		PrevLogTerm:  a.prevTerm,
		CommitIndex:  a.raft.CommitIndex(),
	}
}

func (a *memberAppender) entriesAppendRequest() *raft.AppendRequest {
	prevIndex := a.nextIndex - 1
	if a.prevTerm == 0 && prevIndex >= a.reader.FirstIndex() {
		a.reader.Reset(prevIndex)
		a.prevTerm = a.reader.NextEntry().Entry.Term
	}
	request := &raft.AppendRequest{
		Term:         a.raft.Term(),
		Leader:       a.raft.Member(),
		PrevLogIndex: a.nextIndex - 1,
		PrevLogTerm:  a.prevTerm,
		CommitIndex:  a.raft.CommitIndex(),
	}

	entriesList := list.New()

	// Build a list of entries starting at the nextIndex, using the cache if possible.
	size := 0
	nextIndex := a.nextIndex
	for nextIndex <= a.reader.LastIndex() {
		// First, try to get the entry from the cache.
		a.mu.Lock()
		entry := a.queue.Front()
		if entry != nil {
			indexed := entry.Value.(*log.Entry)
			if indexed.Index == nextIndex {
				entriesList.PushBack(indexed.Entry)
				a.queue.Remove(entry)
				size += indexed.Entry.XXX_Size()
				nextIndex++
				if size >= maxBatchSize {
					break
				}
				a.mu.Unlock()
				continue
			} else if indexed.Index < nextIndex {
				a.queue.Remove(entry)
				a.mu.Unlock()
				continue
			}
		}
		a.mu.Unlock()

		// If the entry was not in the cache, read it from the log reader.
		a.reader.Reset(nextIndex)
		indexed := a.reader.NextEntry()
		if indexed != nil {
			entriesList.PushBack(indexed.Entry)
			size += indexed.Entry.XXX_Size()
			nextIndex++
			if size >= maxBatchSize {
				break
			}
		} else {
			break
		}
	}

	// Convert the linked list into a slice
	entries := make([]*raft.RaftLogEntry, 0, entriesList.Len())
	entry := entriesList.Front()
	for entry != nil {
		entries = append(entries, entry.Value.(*raft.RaftLogEntry))
		entry = entry.Next()
	}

	// Add the entries to the request builder and return the request.
	request.Entries = entries
	return request
}

func (a *memberAppender) sendAppendRequest(request *raft.AppendRequest) {
	// Start the append to the member.
	startTime := time.Now()

	client, err := a.raft.Connect(a.member.MemberID)
	if err != nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), a.raft.Config().GetElectionTimeoutOrDefault())
	defer cancel()

	a.log.SendTo("AppendRequest", request, a.member.MemberID)
	response, err := client.Append(ctx, request)

	if err == nil {
		a.log.ReceiveFrom("AppendResponse", response, a.member.MemberID)
		if response.Status == raft.ResponseStatus_OK {
			a.handleAppendResponse(request, response, startTime)
		} else {
			a.handleAppendFailure(request, response, startTime)
		}
	} else {
		a.log.ErrorFrom("AppendRequest", err, a.member.MemberID)
		a.handleAppendError(request, err, startTime)
	}
}

func (a *memberAppender) commit(time time.Time) {
	// Send a commit event to the parent appender.
	a.commitCh <- memberCommit{
		member: a,
		index:  a.matchIndex,
		time:   time,
	}
}

func (a *memberAppender) handleAppendResponse(request *raft.AppendRequest, response *raft.AppendResponse, startTime time.Time) {
	// Reset the member failure count to avoid empty heartbeats.
	a.succeed()

	// If replication succeeded then trigger commit futures.
	if response.Succeeded {
		// If the replica returned a valid match index then update the existing match index.
		a.matchIndex = response.LastLogIndex
		a.nextIndex = a.matchIndex + 1

		// If entries were sent to the follower, update the previous entry term to the term of the
		// last entry in the follower's log.
		if len(request.Entries) > 0 {
			a.prevTerm = request.Entries[response.LastLogIndex-request.PrevLogIndex-1].Term
		}

		// Send a commit event to the parent appender.
		a.commit(startTime)

		// Notify the appender that the next index can be appended.
		a.requeue()
	} else {
		// If the request was rejected, use a double checked lock to compare the response term to the
		// server's term. If the term is greater than the local server's term, transition back to follower.
		a.raft.ReadLock()
		if response.Term > a.raft.Term() {
			a.raft.ReadUnlock()
			a.raft.WriteLock()
			defer a.raft.WriteUnlock()
			if response.Term > a.raft.Term() {
				// If we've received a greater term, update the term and transition back to follower.
				_ = a.raft.SetTerm(response.Term)
				_ = a.raft.SetLeader(nil)
				go a.raft.SetRole(newFollowerRole(a.raft, a.sm, a.store))
				return
			}
			return
		}

		a.raft.ReadUnlock()

		// If the request was rejected, the follower should have provided the correct last index in their log.
		// This helps us converge on the matchIndex faster than by simply decrementing nextIndex one index at a time.
		// Reset the matchIndex and nextIndex according to the response.
		if response.LastLogIndex < a.matchIndex {
			a.matchIndex = response.LastLogIndex
			a.log.Trace("Reset match index for %s to %d", a.member.MemberID, a.matchIndex)
		}
		if response.LastLogIndex+1 != a.nextIndex {
			a.nextIndex = response.LastLogIndex + 1
			a.log.Trace("Reset next index for %s to %d", a.member.MemberID, a.nextIndex)
			a.prevTerm = 0
		}

		// Notify the appender that the next index can be appended.
		a.requeue()
	}
}

func (a *memberAppender) handleAppendFailure(request *raft.AppendRequest, response *raft.AppendResponse, startTime time.Time) {
	a.fail(startTime)
	a.requeue()
}

func (a *memberAppender) handleAppendError(request *raft.AppendRequest, err error, startTime time.Time) {
	a.fail(startTime)
	a.requeue()
}
