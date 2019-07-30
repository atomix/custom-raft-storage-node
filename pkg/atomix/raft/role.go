package raft

import (
	"context"
)

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
	raft   *RaftServer
	active bool
}

// start starts the role
func (r *raftRole) start() error {
	r.active = true
	return nil
}

// stop stops the role
func (r *raftRole) stop() error {
	r.active = false
	return nil
}

func (r *raftRole) Join(ctx context.Context, request *JoinRequest) (*JoinResponse, error) {
	r.raft.logRequest("JoinRequest", request)
	response := &JoinResponse{
		Status: ResponseStatus_ERROR,
		Error:  RaftError_ILLEGAL_MEMBER_STATE,
	}
	r.raft.logResponse("JoinResponse", response, nil)
	return response, nil
}

func (r *raftRole) Leave(ctx context.Context, request *LeaveRequest) (*LeaveResponse, error) {
	r.raft.logRequest("LeaveRequest", request)
	response := &LeaveResponse{
		Status: ResponseStatus_ERROR,
		Error:  RaftError_ILLEGAL_MEMBER_STATE,
	}
	r.raft.logResponse("LeaveResponse", response, nil)
	return response, nil
}

func (r *raftRole) Configure(ctx context.Context, request *ConfigureRequest) (*ConfigureResponse, error) {
	r.raft.logRequest("ConfigureRequest", request)
	response := &ConfigureResponse{
		Status: ResponseStatus_ERROR,
		Error:  RaftError_ILLEGAL_MEMBER_STATE,
	}
	r.raft.logResponse("", response, nil)
	return response, nil
}

func (r *raftRole) Reconfigure(ctx context.Context, request *ReconfigureRequest) (*ReconfigureResponse, error) {
	r.raft.logRequest("ReconfigureRequest", request)
	response := &ReconfigureResponse{
		Status: ResponseStatus_ERROR,
		Error:  RaftError_ILLEGAL_MEMBER_STATE,
	}
	r.raft.logResponse("ReconfigureResponse", response, nil)
	return response, nil
}

func (r *raftRole) Poll(ctx context.Context, request *PollRequest) (*PollResponse, error) {
	r.raft.logRequest("PollRequest", request)
	response := &PollResponse{
		Status: ResponseStatus_ERROR,
		Error:  RaftError_ILLEGAL_MEMBER_STATE,
	}
	r.raft.logResponse("PollResponse", response, nil)
	return response, nil
}

func (r *raftRole) Vote(ctx context.Context, request *VoteRequest) (*VoteResponse, error) {
	r.raft.logRequest("VoteRequest", request)
	response := &VoteResponse{
		Status: ResponseStatus_ERROR,
		Error:  RaftError_ILLEGAL_MEMBER_STATE,
	}
	r.raft.logResponse("VoteResponse", response, nil)
	return response, nil
}

func (r *raftRole) Transfer(ctx context.Context, request *TransferRequest) (*TransferResponse, error) {
	r.raft.logRequest("TransferRequest", request)
	response := &TransferResponse{
		Status: ResponseStatus_ERROR,
		Error:  RaftError_ILLEGAL_MEMBER_STATE,
	}
	r.raft.logResponse("TransferResponse", response, nil)
	return response, nil
}

func (r *raftRole) Append(ctx context.Context, request *AppendRequest) (*AppendResponse, error) {
	r.raft.logRequest("AppendRequest", request)
	response := &AppendResponse{
		Status: ResponseStatus_ERROR,
		Error:  RaftError_ILLEGAL_MEMBER_STATE,
	}
	r.raft.logResponse("AppendResponse", response, nil)
	return response, nil
}

func (r *raftRole) Install(server RaftService_InstallServer) error {
	response := &InstallResponse{
		Status: ResponseStatus_ERROR,
		Error:  RaftError_ILLEGAL_MEMBER_STATE,
	}
	return r.raft.logResponse("InstallResponse", response, server.SendAndClose(response))
}

func (r *raftRole) Command(request *CommandRequest, server RaftService_CommandServer) error {
	r.raft.logRequest("CommandRequest", request)
	response := &CommandResponse{
		Status: ResponseStatus_ERROR,
		Error:  RaftError_ILLEGAL_MEMBER_STATE,
	}
	return r.raft.logResponse("CommandResponse", response, server.Send(response))
}

func (r *raftRole) Query(request *QueryRequest, server RaftService_QueryServer) error {
	r.raft.logRequest("QueryRequest", request)
	response := &QueryResponse{
		Status: ResponseStatus_ERROR,
		Error:  RaftError_ILLEGAL_MEMBER_STATE,
	}
	return r.raft.logResponse("QueryResponse", response, server.Send(response))
}
