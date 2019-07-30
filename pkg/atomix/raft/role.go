package raft

import (
	"context"
)

func newRaftRole(server *RaftServer) *raftRole {
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
	server *RaftServer
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

func (r *raftRole) Join(ctx context.Context, request *JoinRequest) (*JoinResponse, error) {
	r.server.logRequest("JoinRequest", request)
	response := &JoinResponse{
		Status: ResponseStatus_ERROR,
		Error:  RaftError_ILLEGAL_MEMBER_STATE,
	}
	r.server.logResponse("JoinResponse", response, nil)
	return response, nil
}

func (r *raftRole) Leave(ctx context.Context, request *LeaveRequest) (*LeaveResponse, error) {
	r.server.logRequest("LeaveRequest", request)
	response := &LeaveResponse{
		Status: ResponseStatus_ERROR,
		Error:  RaftError_ILLEGAL_MEMBER_STATE,
	}
	r.server.logResponse("LeaveResponse", response, nil)
	return response, nil
}

func (r *raftRole) Configure(ctx context.Context, request *ConfigureRequest) (*ConfigureResponse, error) {
	r.server.logRequest("ConfigureRequest", request)
	response := &ConfigureResponse{
		Status: ResponseStatus_ERROR,
		Error:  RaftError_ILLEGAL_MEMBER_STATE,
	}
	r.server.logResponse("", response, nil)
	return response, nil
}

func (r *raftRole) Reconfigure(ctx context.Context, request *ReconfigureRequest) (*ReconfigureResponse, error) {
	r.server.logRequest("ReconfigureRequest", request)
	response := &ReconfigureResponse{
		Status: ResponseStatus_ERROR,
		Error:  RaftError_ILLEGAL_MEMBER_STATE,
	}
	r.server.logResponse("ReconfigureResponse", response, nil)
	return response, nil
}

func (r *raftRole) Poll(ctx context.Context, request *PollRequest) (*PollResponse, error) {
	r.server.logRequest("PollRequest", request)
	response := &PollResponse{
		Status: ResponseStatus_ERROR,
		Error:  RaftError_ILLEGAL_MEMBER_STATE,
	}
	r.server.logResponse("PollResponse", response, nil)
	return response, nil
}

func (r *raftRole) Vote(ctx context.Context, request *VoteRequest) (*VoteResponse, error) {
	r.server.logRequest("VoteRequest", request)
	response := &VoteResponse{
		Status: ResponseStatus_ERROR,
		Error:  RaftError_ILLEGAL_MEMBER_STATE,
	}
	r.server.logResponse("VoteResponse", response, nil)
	return response, nil
}

func (r *raftRole) Transfer(ctx context.Context, request *TransferRequest) (*TransferResponse, error) {
	r.server.logRequest("TransferRequest", request)
	response := &TransferResponse{
		Status: ResponseStatus_ERROR,
		Error:  RaftError_ILLEGAL_MEMBER_STATE,
	}
	r.server.logResponse("TransferResponse", response, nil)
	return response, nil
}

func (r *raftRole) Append(ctx context.Context, request *AppendRequest) (*AppendResponse, error) {
	r.server.logRequest("AppendRequest", request)
	response := &AppendResponse{
		Status: ResponseStatus_ERROR,
		Error:  RaftError_ILLEGAL_MEMBER_STATE,
	}
	r.server.logResponse("AppendResponse", response, nil)
	return response, nil
}

func (r *raftRole) Install(server RaftService_InstallServer) error {
	response := &InstallResponse{
		Status: ResponseStatus_ERROR,
		Error:  RaftError_ILLEGAL_MEMBER_STATE,
	}
	return r.server.logResponse("InstallResponse", response, server.SendAndClose(response))
}

func (r *raftRole) Command(request *CommandRequest, server RaftService_CommandServer) error {
	r.server.logRequest("CommandRequest", request)
	response := &CommandResponse{
		Status: ResponseStatus_ERROR,
		Error:  RaftError_ILLEGAL_MEMBER_STATE,
	}
	return r.server.logResponse("CommandResponse", response, server.Send(response))
}

func (r *raftRole) Query(request *QueryRequest, server RaftService_QueryServer) error {
	r.server.logRequest("QueryRequest", request)
	response := &QueryResponse{
		Status: ResponseStatus_ERROR,
		Error:  RaftError_ILLEGAL_MEMBER_STATE,
	}
	return r.server.logResponse("QueryResponse", response, server.Send(response))
}
