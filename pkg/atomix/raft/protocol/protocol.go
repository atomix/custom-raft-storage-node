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

package protocol

import (
	"context"
	"errors"
	"io"
)

// NewClient creates a new Raft protocol client
func NewClient(cluster Cluster) Client {
	return &gRPCClient{cluster}
}

// NewServer creates a new RaftServiceServer for the given Server
func NewServer(server Server) RaftServiceServer {
	return &gRPCServer{server}
}

// Client is an interface for sending messages to Raft nodes
type Client interface {
	// Join sends a join request
	Join(ctx context.Context, request *JoinRequest, member MemberID) (*JoinResponse, error)

	// Leave sends a leave request
	Leave(ctx context.Context, request *LeaveRequest, member MemberID) (*LeaveResponse, error)

	// Configure sends a configure request
	Configure(ctx context.Context, request *ConfigureRequest, member MemberID) (*ConfigureResponse, error)

	// Reconfigure sends a reconfigure request
	Reconfigure(ctx context.Context, request *ReconfigureRequest, member MemberID) (*ReconfigureResponse, error)

	// Poll sends a poll request
	Poll(ctx context.Context, request *PollRequest, member MemberID) (*PollResponse, error)

	// Vote sends a vote request
	Vote(ctx context.Context, request *VoteRequest, member MemberID) (*VoteResponse, error)

	// Transfer sends a leadership transfer request
	Transfer(ctx context.Context, request *TransferRequest, member MemberID) (*TransferResponse, error)

	// Append sends an append request
	Append(ctx context.Context, request *AppendRequest, member MemberID) (*AppendResponse, error)

	// Install sends a stream of install requests
	Install(ctx context.Context, member MemberID) (chan<- *InstallRequest, <-chan *InstallStreamResponse, error)

	// Command sends a command request
	Command(ctx context.Context, request *CommandRequest, member MemberID) (<-chan *CommandStreamResponse, error)

	// Query sends a query request
	Query(ctx context.Context, request *QueryRequest, member MemberID) (<-chan *QueryStreamResponse, error)
}

// Server is an interface for receiving Raft messages
type Server interface {
	// Join handles a join request
	Join(ctx context.Context, request *JoinRequest) (*JoinResponse, error)

	// Leave handles a leave request
	Leave(ctx context.Context, request *LeaveRequest) (*LeaveResponse, error)

	// Configure handles a configure request
	Configure(ctx context.Context, request *ConfigureRequest) (*ConfigureResponse, error)

	// Reconfigure handles a reconfigure request
	Reconfigure(ctx context.Context, request *ReconfigureRequest) (*ReconfigureResponse, error)

	// Poll handles a poll request
	Poll(ctx context.Context, request *PollRequest) (*PollResponse, error)

	// Vote handles a vote request
	Vote(ctx context.Context, request *VoteRequest) (*VoteResponse, error)

	// Transfer handles a leadership transfer request
	Transfer(ctx context.Context, request *TransferRequest) (*TransferResponse, error)

	// Append handles an append request
	Append(ctx context.Context, request *AppendRequest) (*AppendResponse, error)

	// Install handles an install request
	Install(ch <-chan *InstallStreamRequest) (*InstallResponse, error)

	// Command handles a command request
	Command(request *CommandRequest, ch chan<- *CommandStreamResponse) error

	// Query handles a query request
	Query(request *QueryRequest, ch chan<- *QueryStreamResponse) error
}

// StreamMessage is a stream message/error pair
type StreamMessage struct {
	Error error
}

// Succeeded returns a bool indicating whether the request succeeded
func (r *StreamMessage) Succeeded() bool {
	return r.Error == nil
}

// Failed returns a bool indicating whether the request failed
func (r *StreamMessage) Failed() bool {
	return r.Error != nil
}

// NewInstallStreamRequest returns a new InstallStreamRequest with the given request and error
func NewInstallStreamRequest(request *InstallRequest, err error) *InstallStreamRequest {
	return &InstallStreamRequest{
		StreamMessage: &StreamMessage{
			Error: err,
		},
		Request: request,
	}
}

// InstallStreamRequest is a stream request for InstallRequest
type InstallStreamRequest struct {
	*StreamMessage
	Request *InstallRequest
}

// NewInstallStreamResponse returns a new InstallStreamResponse with the given request and error
func NewInstallStreamResponse(response *InstallResponse, err error) *InstallStreamResponse {
	return &InstallStreamResponse{
		StreamMessage: &StreamMessage{
			Error: err,
		},
		Response: response,
	}
}

// InstallStreamResponse is a stream request for InstallResponse
type InstallStreamResponse struct {
	*StreamMessage
	Response *InstallResponse
}

// NewCommandStreamResponse returns a new CommandStreamResponse with the given response and error
func NewCommandStreamResponse(response *CommandResponse, err error) *CommandStreamResponse {
	return &CommandStreamResponse{
		StreamMessage: &StreamMessage{
			Error: err,
		},
		Response: response,
	}
}

// CommandStreamResponse is a stream response for CommandRequest
type CommandStreamResponse struct {
	*StreamMessage
	Response *CommandResponse
}

// NewQueryStreamResponse returns a new CommandStreamResponse with the given response and error
func NewQueryStreamResponse(response *QueryResponse, err error) *QueryStreamResponse {
	return &QueryStreamResponse{
		StreamMessage: &StreamMessage{
			Error: err,
		},
		Response: response,
	}
}

// QueryStreamResponse is a stream response for QueryRequest
type QueryStreamResponse struct {
	*StreamMessage
	Response *QueryResponse
}

// gRPCServer implements the gRPC server interface to proxy calls to Servers
type gRPCServer struct {
	server Server
}

func (s *gRPCServer) Join(ctx context.Context, request *JoinRequest) (*JoinResponse, error) {
	return s.server.Join(ctx, request)
}

func (s *gRPCServer) Leave(ctx context.Context, request *LeaveRequest) (*LeaveResponse, error) {
	return s.server.Leave(ctx, request)
}

func (s *gRPCServer) Configure(ctx context.Context, request *ConfigureRequest) (*ConfigureResponse, error) {
	return s.server.Configure(ctx, request)
}

func (s *gRPCServer) Reconfigure(ctx context.Context, request *ReconfigureRequest) (*ReconfigureResponse, error) {
	return s.server.Reconfigure(ctx, request)
}

func (s *gRPCServer) Poll(ctx context.Context, request *PollRequest) (*PollResponse, error) {
	return s.server.Poll(ctx, request)
}

func (s *gRPCServer) Vote(ctx context.Context, request *VoteRequest) (*VoteResponse, error) {
	return s.server.Vote(ctx, request)
}

func (s *gRPCServer) Transfer(ctx context.Context, request *TransferRequest) (*TransferResponse, error) {
	return s.server.Transfer(ctx, request)
}

func (s *gRPCServer) Append(ctx context.Context, request *AppendRequest) (*AppendResponse, error) {
	return s.server.Append(ctx, request)
}

func (s *gRPCServer) Install(stream RaftService_InstallServer) error {
	ch := make(chan *InstallStreamRequest)
	go func() {
		for {
			request, err := stream.Recv()
			if err != nil {
				if err != io.EOF {
					ch <- NewInstallStreamRequest(nil, err)
				}
				close(ch)
				break
			} else {
				ch <- NewInstallStreamRequest(request, nil)
			}
		}
	}()

	response, err := s.server.Install(ch)
	if err != nil {
		return err
	}
	return stream.SendAndClose(response)
}

func (s *gRPCServer) Command(request *CommandRequest, stream RaftService_CommandServer) error {
	responseCh := make(chan *CommandStreamResponse)
	errCh := make(chan error)
	go func() {
		for response := range responseCh {
			if response.Failed() {
				errCh <- response.Error
			} else if err := stream.Send(response.Response); err != nil {
				errCh <- response.Error
			}
		}
		close(errCh)
	}()
	if err := s.server.Command(request, responseCh); err != nil {
		return err
	}

	err, ok := <-errCh
	if ok {
		return err
	}
	return nil
}

func (s *gRPCServer) Query(request *QueryRequest, stream RaftService_QueryServer) error {
	responseCh := make(chan *QueryStreamResponse)
	errCh := make(chan error)
	go func() {
		for response := range responseCh {
			if response.Failed() {
				errCh <- response.Error
			} else if err := stream.Send(response.Response); err != nil {
				errCh <- response.Error
			}
		}
		close(errCh)
	}()
	if err := s.server.Query(request, responseCh); err != nil {
		return err
	}

	err, ok := <-errCh
	if ok {
		return err
	}
	return nil
}

// gRPCClient uses gRPC clients to send messages to remote nodes
type gRPCClient struct {
	cluster Cluster
}

func (p *gRPCClient) Join(ctx context.Context, request *JoinRequest, member MemberID) (*JoinResponse, error) {
	client, err := p.cluster.GetClient(member)
	if err != nil {
		return nil, err
	}
	return client.Join(ctx, request)
}

func (p *gRPCClient) Leave(ctx context.Context, request *LeaveRequest, member MemberID) (*LeaveResponse, error) {
	client, err := p.cluster.GetClient(member)
	if err != nil {
		return nil, err
	}
	return client.Leave(ctx, request)
}

func (p *gRPCClient) Configure(ctx context.Context, request *ConfigureRequest, member MemberID) (*ConfigureResponse, error) {
	client, err := p.cluster.GetClient(member)
	if err != nil {
		return nil, err
	}
	return client.Configure(ctx, request)
}

func (p *gRPCClient) Reconfigure(ctx context.Context, request *ReconfigureRequest, member MemberID) (*ReconfigureResponse, error) {
	client, err := p.cluster.GetClient(member)
	if err != nil {
		return nil, err
	}
	return client.Reconfigure(ctx, request)
}

func (p *gRPCClient) Poll(ctx context.Context, request *PollRequest, member MemberID) (*PollResponse, error) {
	client, err := p.cluster.GetClient(member)
	if err != nil {
		return nil, err
	}
	return client.Poll(ctx, request)
}

func (p *gRPCClient) Vote(ctx context.Context, request *VoteRequest, member MemberID) (*VoteResponse, error) {
	client, err := p.cluster.GetClient(member)
	if err != nil {
		return nil, err
	}
	return client.Vote(ctx, request)
}

func (p *gRPCClient) Transfer(ctx context.Context, request *TransferRequest, member MemberID) (*TransferResponse, error) {
	client, err := p.cluster.GetClient(member)
	if err != nil {
		return nil, err
	}
	return client.Transfer(ctx, request)
}

func (p *gRPCClient) Append(ctx context.Context, request *AppendRequest, member MemberID) (*AppendResponse, error) {
	client, err := p.cluster.GetClient(member)
	if err != nil {
		return nil, err
	}
	return client.Append(ctx, request)
}

func (p *gRPCClient) Install(ctx context.Context, member MemberID) (chan<- *InstallRequest, <-chan *InstallStreamResponse, error) {
	client, err := p.cluster.GetClient(member)
	if err != nil {
		return nil, nil, err
	}
	stream, err := client.Install(ctx)
	if err != nil {
		return nil, nil, err
	}

	requestCh := make(chan *InstallRequest)
	responseCh := make(chan *InstallStreamResponse)
	go func() {
		for request := range requestCh {
			if err := stream.Send(request); err != nil {
				responseCh <- NewInstallStreamResponse(nil, err)
				close(responseCh)
				return
			}
		}

		responseCh <- NewInstallStreamResponse(stream.CloseAndRecv())
		close(responseCh)
	}()
	return requestCh, responseCh, nil
}

func (p *gRPCClient) Command(ctx context.Context, request *CommandRequest, member MemberID) (<-chan *CommandStreamResponse, error) {
	client, err := p.cluster.GetClient(member)
	if err != nil {
		return nil, err
	}

	stream, err := client.Command(ctx, request)
	if err != nil {
		return nil, err
	}

	ch := make(chan *CommandStreamResponse)
	go func() {
		for {
			response, err := stream.Recv()
			if err == io.EOF {
				close(ch)
				break
			} else if err != nil {
				ch <- &CommandStreamResponse{
					StreamMessage: &StreamMessage{Error: err},
				}
				break
			}

			ch <- &CommandStreamResponse{
				Response: response,
			}
		}
	}()
	return ch, nil
}

func (p *gRPCClient) Query(ctx context.Context, request *QueryRequest, member MemberID) (<-chan *QueryStreamResponse, error) {
	client, err := p.cluster.GetClient(member)
	if err != nil {
		return nil, err
	}

	stream, err := client.Query(ctx, request)
	if err != nil {
		return nil, err
	}

	ch := make(chan *QueryStreamResponse)
	go func() {
		for {
			response, err := stream.Recv()
			if err == io.EOF {
				close(ch)
				break
			} else if err != nil {
				ch <- &QueryStreamResponse{
					StreamMessage: &StreamMessage{Error: err},
				}
				break
			}

			ch <- &QueryStreamResponse{
				Response: response,
			}
		}
	}()
	return ch, nil
}

// UnimplementedClient is a Client implementation that supports overrides of individual protocol methods
type UnimplementedClient struct {
}

// Join sends a join request to the given member
func (p *UnimplementedClient) Join(ctx context.Context, request *JoinRequest, member MemberID) (*JoinResponse, error) {
	return nil, errors.New("not implemented")
}

// Leave sends a leave request to the given member
func (p *UnimplementedClient) Leave(ctx context.Context, request *LeaveRequest, member MemberID) (*LeaveResponse, error) {
	return nil, errors.New("not implemented")
}

// Configure sends a configure request to the given member
func (p *UnimplementedClient) Configure(ctx context.Context, request *ConfigureRequest, member MemberID) (*ConfigureResponse, error) {
	return nil, errors.New("not implemented")
}

// Reconfigure sends a reconfigure request to the given member
func (p *UnimplementedClient) Reconfigure(ctx context.Context, request *ReconfigureRequest, member MemberID) (*ReconfigureResponse, error) {
	return nil, errors.New("not implemented")
}

// Poll sends a poll request to the given member
func (p *UnimplementedClient) Poll(ctx context.Context, request *PollRequest, member MemberID) (*PollResponse, error) {
	return nil, errors.New("not implemented")
}

// Vote sends a vote request to the given member
func (p *UnimplementedClient) Vote(ctx context.Context, request *VoteRequest, member MemberID) (*VoteResponse, error) {
	return nil, errors.New("not implemented")
}

// Transfer sends a transfer request to the given member
func (p *UnimplementedClient) Transfer(ctx context.Context, request *TransferRequest, member MemberID) (*TransferResponse, error) {
	return nil, errors.New("not implemented")
}

// Append sends an append request to the given member
func (p *UnimplementedClient) Append(ctx context.Context, request *AppendRequest, member MemberID) (*AppendResponse, error) {
	return nil, errors.New("not implemented")
}

// Install sends a stream of install requests to the given member
func (p *UnimplementedClient) Install(ctx context.Context, member MemberID) (chan<- *InstallRequest, <-chan *InstallStreamResponse, error) {
	return nil, nil, errors.New("not implemented")
}

// Command sends a command request to the given member
func (p *UnimplementedClient) Command(ctx context.Context, request *CommandRequest, member MemberID) (<-chan *CommandStreamResponse, error) {
	return nil, errors.New("not implemented")
}

// Query sends a query request to the given member
func (p *UnimplementedClient) Query(ctx context.Context, request *QueryRequest, member MemberID) (<-chan *QueryStreamResponse, error) {
	return nil, errors.New("not implemented")
}
