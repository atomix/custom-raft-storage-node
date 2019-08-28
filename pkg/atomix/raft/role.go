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

package raft

import (
	"context"
)

func newRaftRole(server *Server) *raftRole {
	return &raftRole{
		server: server,
		active: true,
	}
}

// Role is implemented by server roles to support protocol requests
type Role interface {
	RaftServiceServer

	// Name is the name of the role
	Name() string

	// start initializes the role
	start() error

	// stop stops the role
	stop() error
}

// raftRole is the base role for all Raft Role implementations
type raftRole struct {
	server *Server
	active bool
}

// start starts the role
func (r *raftRole) start() error {
	return nil
}

// stop stops the role
func (r *raftRole) stop() error {
	r.active = false
	return nil
}

// Join handles a join request
func (r *raftRole) Join(ctx context.Context, request *JoinRequest) (*JoinResponse, error) {
	r.server.logRequest("JoinRequest", request)
	response := &JoinResponse{
		Status: ResponseStatus_ERROR,
		Error:  RaftError_ILLEGAL_MEMBER_STATE,
	}
	_ = r.server.logResponse("JoinResponse", response, nil)
	return response, nil
}

// Leave handles a leave request
func (r *raftRole) Leave(ctx context.Context, request *LeaveRequest) (*LeaveResponse, error) {
	r.server.logRequest("LeaveRequest", request)
	response := &LeaveResponse{
		Status: ResponseStatus_ERROR,
		Error:  RaftError_ILLEGAL_MEMBER_STATE,
	}
	_ = r.server.logResponse("LeaveResponse", response, nil)
	return response, nil
}

// Configure handles a configure request
func (r *raftRole) Configure(ctx context.Context, request *ConfigureRequest) (*ConfigureResponse, error) {
	r.server.logRequest("ConfigureRequest", request)
	response := &ConfigureResponse{
		Status: ResponseStatus_ERROR,
		Error:  RaftError_ILLEGAL_MEMBER_STATE,
	}
	_ = r.server.logResponse("", response, nil)
	return response, nil
}

// Reconfigure handles a reconfigure request
func (r *raftRole) Reconfigure(ctx context.Context, request *ReconfigureRequest) (*ReconfigureResponse, error) {
	r.server.logRequest("ReconfigureRequest", request)
	response := &ReconfigureResponse{
		Status: ResponseStatus_ERROR,
		Error:  RaftError_ILLEGAL_MEMBER_STATE,
	}
	_ = r.server.logResponse("ReconfigureResponse", response, nil)
	return response, nil
}

// Poll handles a poll request
func (r *raftRole) Poll(ctx context.Context, request *PollRequest) (*PollResponse, error) {
	r.server.logRequest("PollRequest", request)
	response := &PollResponse{
		Status: ResponseStatus_ERROR,
		Error:  RaftError_ILLEGAL_MEMBER_STATE,
	}
	_ = r.server.logResponse("PollResponse", response, nil)
	return response, nil
}

// Vote handles a vote request
func (r *raftRole) Vote(ctx context.Context, request *VoteRequest) (*VoteResponse, error) {
	r.server.logRequest("VoteRequest", request)
	response := &VoteResponse{
		Status: ResponseStatus_ERROR,
		Error:  RaftError_ILLEGAL_MEMBER_STATE,
	}
	_ = r.server.logResponse("VoteResponse", response, nil)
	return response, nil
}

// Transfer handles a transfer request
func (r *raftRole) Transfer(ctx context.Context, request *TransferRequest) (*TransferResponse, error) {
	r.server.logRequest("TransferRequest", request)
	response := &TransferResponse{
		Status: ResponseStatus_ERROR,
		Error:  RaftError_ILLEGAL_MEMBER_STATE,
	}
	_ = r.server.logResponse("TransferResponse", response, nil)
	return response, nil
}

// Append handles a append request
func (r *raftRole) Append(ctx context.Context, request *AppendRequest) (*AppendResponse, error) {
	r.server.logRequest("AppendRequest", request)
	response := &AppendResponse{
		Status: ResponseStatus_ERROR,
		Error:  RaftError_ILLEGAL_MEMBER_STATE,
	}
	_ = r.server.logResponse("AppendResponse", response, nil)
	return response, nil
}

// Install handles an install request
func (r *raftRole) Install(server RaftService_InstallServer) error {
	response := &InstallResponse{
		Status: ResponseStatus_ERROR,
		Error:  RaftError_ILLEGAL_MEMBER_STATE,
	}
	return r.server.logResponse("InstallResponse", response, server.SendAndClose(response))
}

// Command handles a command request
func (r *raftRole) Command(request *CommandRequest, server RaftService_CommandServer) error {
	r.server.logRequest("CommandRequest", request)
	response := &CommandResponse{
		Status: ResponseStatus_ERROR,
		Error:  RaftError_ILLEGAL_MEMBER_STATE,
	}
	return r.server.logResponse("CommandResponse", response, server.Send(response))
}

// Query handles a query request
func (r *raftRole) Query(request *QueryRequest, server RaftService_QueryServer) error {
	r.server.logRequest("QueryRequest", request)
	response := &QueryResponse{
		Status: ResponseStatus_ERROR,
		Error:  RaftError_ILLEGAL_MEMBER_STATE,
	}
	return r.server.logResponse("QueryResponse", response, server.Send(response))
}
