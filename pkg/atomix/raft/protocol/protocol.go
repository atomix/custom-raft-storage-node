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

// NewProtocol creates a new gRPC protocol
func NewProtocol(cluster Cluster) Protocol {
	return &gRPCProtocol{cluster}
}

// Protocol is an interface for sending messages to Raft nodes
type Protocol interface {
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

	// Install sends an install request
	Install(ctx context.Context, member MemberID, ch <-chan *InstallRequest) (*InstallResponse, error)

	// Command sends a command request
	Command(ctx context.Context, request *CommandRequest, member MemberID) (<-chan *CommandStreamResponse, error)

	// Query sends a query request
	Query(ctx context.Context, request *QueryRequest, member MemberID) (<-chan *QueryStreamResponse, error)
}

// StreamResponse is a stream response/error pair
type StreamResponse struct {
	Error error
}

// Succeeded returns a bool indicating whether the request succeeded
func (r *StreamResponse) Succeeded() bool {
	return r.Error == nil
}

// Failed returns a bool indicating whether the request failed
func (r *StreamResponse) Failed() bool {
	return r.Error != nil
}

// CommandStreamResponse is a stream response for CommandRequest
type CommandStreamResponse struct {
	*StreamResponse
	Response *CommandResponse
}

// QueryStreamResponse is a stream response for QueryRequest
type QueryStreamResponse struct {
	*StreamResponse
	Response *QueryResponse
}

// gRPCProtocol uses gRPC clients to send messages to remote nodes
type gRPCProtocol struct {
	cluster Cluster
}

func (p *gRPCProtocol) Join(ctx context.Context, request *JoinRequest, member MemberID) (*JoinResponse, error) {
	client, err := p.cluster.GetClient(member)
	if err != nil {
		return nil, err
	}
	return client.Join(ctx, request)
}

func (p *gRPCProtocol) Leave(ctx context.Context, request *LeaveRequest, member MemberID) (*LeaveResponse, error) {
	client, err := p.cluster.GetClient(member)
	if err != nil {
		return nil, err
	}
	return client.Leave(ctx, request)
}

func (p *gRPCProtocol) Configure(ctx context.Context, request *ConfigureRequest, member MemberID) (*ConfigureResponse, error) {
	client, err := p.cluster.GetClient(member)
	if err != nil {
		return nil, err
	}
	return client.Configure(ctx, request)
}

func (p *gRPCProtocol) Reconfigure(ctx context.Context, request *ReconfigureRequest, member MemberID) (*ReconfigureResponse, error) {
	client, err := p.cluster.GetClient(member)
	if err != nil {
		return nil, err
	}
	return client.Reconfigure(ctx, request)
}

func (p *gRPCProtocol) Poll(ctx context.Context, request *PollRequest, member MemberID) (*PollResponse, error) {
	client, err := p.cluster.GetClient(member)
	if err != nil {
		return nil, err
	}
	return client.Poll(ctx, request)
}

func (p *gRPCProtocol) Vote(ctx context.Context, request *VoteRequest, member MemberID) (*VoteResponse, error) {
	client, err := p.cluster.GetClient(member)
	if err != nil {
		return nil, err
	}
	return client.Vote(ctx, request)
}

func (p *gRPCProtocol) Transfer(ctx context.Context, request *TransferRequest, member MemberID) (*TransferResponse, error) {
	client, err := p.cluster.GetClient(member)
	if err != nil {
		return nil, err
	}
	return client.Transfer(ctx, request)
}

func (p *gRPCProtocol) Append(ctx context.Context, request *AppendRequest, member MemberID) (*AppendResponse, error) {
	client, err := p.cluster.GetClient(member)
	if err != nil {
		return nil, err
	}
	return client.Append(ctx, request)
}

func (p *gRPCProtocol) Install(ctx context.Context, member MemberID, ch <-chan *InstallRequest) (*InstallResponse, error) {
	client, err := p.cluster.GetClient(member)
	if err != nil {
		return nil, err
	}
	stream, err := client.Install(ctx)
	if err != nil {
		return nil, err
	}

	for request := range ch {
		if err := stream.Send(request); err != nil {
			return nil, err
		}
	}
	return stream.CloseAndRecv()
}

func (p *gRPCProtocol) Command(ctx context.Context, request *CommandRequest, member MemberID) (<-chan *CommandStreamResponse, error) {
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
					StreamResponse: &StreamResponse{Error: err},
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

func (p *gRPCProtocol) Query(ctx context.Context, request *QueryRequest, member MemberID) (<-chan *QueryStreamResponse, error) {
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
					StreamResponse: &StreamResponse{Error: err},
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

// UnimplementedProtocol is a Protocol implementation that supports overrides of individual protocol methods
type UnimplementedProtocol struct {
}

// Join sends a join request to the given member
func (p *UnimplementedProtocol) Join(ctx context.Context, request *JoinRequest, member MemberID) (*JoinResponse, error) {
	return nil, errors.New("not implemented")
}

// Leave sends a leave request to the given member
func (p *UnimplementedProtocol) Leave(ctx context.Context, request *LeaveRequest, member MemberID) (*LeaveResponse, error) {
	return nil, errors.New("not implemented")
}

// Configure sends a configure request to the given member
func (p *UnimplementedProtocol) Configure(ctx context.Context, request *ConfigureRequest, member MemberID) (*ConfigureResponse, error) {
	return nil, errors.New("not implemented")
}

// Reconfigure sends a reconfigure request to the given member
func (p *UnimplementedProtocol) Reconfigure(ctx context.Context, request *ReconfigureRequest, member MemberID) (*ReconfigureResponse, error) {
	return nil, errors.New("not implemented")
}

// Poll sends a poll request to the given member
func (p *UnimplementedProtocol) Poll(ctx context.Context, request *PollRequest, member MemberID) (*PollResponse, error) {
	return nil, errors.New("not implemented")
}

// Vote sends a vote request to the given member
func (p *UnimplementedProtocol) Vote(ctx context.Context, request *VoteRequest, member MemberID) (*VoteResponse, error) {
	return nil, errors.New("not implemented")
}

// Transfer sends a transfer request to the given member
func (p *UnimplementedProtocol) Transfer(ctx context.Context, request *TransferRequest, member MemberID) (*TransferResponse, error) {
	return nil, errors.New("not implemented")
}

// Append sends an append request to the given member
func (p *UnimplementedProtocol) Append(ctx context.Context, request *AppendRequest, member MemberID) (*AppendResponse, error) {
	return nil, errors.New("not implemented")
}

// Install sends an install request to the given member
func (p *UnimplementedProtocol) Install(ctx context.Context, member MemberID, ch <-chan *InstallRequest) (*InstallResponse, error) {
	return nil, errors.New("not implemented")
}

// Command sends a command request to the given member
func (p *UnimplementedProtocol) Command(ctx context.Context, request *CommandRequest, member MemberID) (<-chan *CommandStreamResponse, error) {
	return nil, errors.New("not implemented")
}

// Query sends a query request to the given member
func (p *UnimplementedProtocol) Query(ctx context.Context, request *QueryRequest, member MemberID) (<-chan *QueryStreamResponse, error) {
	return nil, errors.New("not implemented")
}
