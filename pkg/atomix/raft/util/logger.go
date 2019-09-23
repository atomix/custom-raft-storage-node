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

package util

import "github.com/atomix/atomix-go-node/pkg/atomix/util"

// NewNodeLogger creates a new role logger
func NewNodeLogger(node string) Logger {
	return &nodeLogger{
		node: node,
	}
}

// NewRoleLogger creates a new role logger
func NewRoleLogger(node string, role string) Logger {
	return &roleLogger{
		nodeLogger: &nodeLogger{
			node: node,
		},
		role: role,
	}
}

// Logger provides logging for requests and responses
type Logger interface {
	// Error logs an Error level message
	Error(message string, args ...interface{})

	// Warn logs a Warn level message
	Warn(message string, args ...interface{})

	// Info logs an Info level message
	Info(message string, args ...interface{})

	// Debug logs a Debug level message
	Debug(message string, args ...interface{})

	// Trace logs a Trace level message
	Trace(message string, args ...interface{})

	// Send logs a sent message
	Send(messageType string, request interface{})

	// Receive logs a received message
	Receive(messageType string, response interface{})

	// SendTo logs a message send to a specific member
	SendTo(messageType string, request interface{}, member interface{})

	// ReceiveFrom logs a message received from a specific member
	ReceiveFrom(messageType string, response interface{}, member interface{})

	// ErrorFrom logs an error received from a specific member
	ErrorFrom(messageType string, err error, member interface{})

	// Request logs a request
	Request(requestType string, request interface{})

	// Response logs a response
	Response(responseType string, response interface{}, err error) error
}

// nodeLogger is a Logger implementation for the Raft protocol
type nodeLogger struct {
	node string
}

func (l *nodeLogger) Error(message string, args ...interface{}) {
	util.NodeEntry(l.node).Errorf(message, args...)
}

func (l *nodeLogger) Warn(message string, args ...interface{}) {
	util.NodeEntry(l.node).Warnf(message, args...)
}

func (l *nodeLogger) Info(message string, args ...interface{}) {
	util.NodeEntry(l.node).Infof(message, args...)
}

func (l *nodeLogger) Debug(message string, args ...interface{}) {
	util.NodeEntry(l.node).Debugf(message, args...)
}

func (l *nodeLogger) Trace(message string, args ...interface{}) {
	util.NodeEntry(l.node).Tracef(message, args...)
}

func (l *nodeLogger) Send(messageType string, request interface{}) {
	_ = l.Response(messageType, request, nil)
}

func (l *nodeLogger) Receive(messageType string, response interface{}) {
	l.Request(messageType, response)
}

func (l *nodeLogger) SendTo(messageType string, request interface{}, member interface{}) {
	util.RequestEntry(l.node, messageType).
		Tracef("Sending %v to %s", request, member)
}

func (l *nodeLogger) ReceiveFrom(messageType string, response interface{}, member interface{}) {
	util.ResponseEntry(l.node, messageType).
		Tracef("Received %v from %s", response, member)
}

func (l *nodeLogger) ErrorFrom(messageType string, err error, member interface{}) {
	util.ResponseEntry(l.node, messageType).
		Tracef("Received error %v from %s", err, member)
}

func (l *nodeLogger) Request(requestType string, request interface{}) {
	util.RequestEntry(l.node, requestType).
		Tracef("Received %v", request)
}

func (l *nodeLogger) Response(responseType string, response interface{}, err error) error {
	util.ResponseEntry(l.node, responseType).
		Tracef("Sending %v", response)
	return err
}

// roleLogger is a Logger implementation for server roles
type roleLogger struct {
	*nodeLogger
	role string
}

func (l *roleLogger) Error(message string, args ...interface{}) {
	util.MessageEntry(l.node, l.role).Errorf(message, args...)
}

func (l *roleLogger) Warn(message string, args ...interface{}) {
	util.MessageEntry(l.node, l.role).Warnf(message, args...)
}

func (l *roleLogger) Info(message string, args ...interface{}) {
	util.MessageEntry(l.node, l.role).Infof(message, args...)
}

func (l *roleLogger) Debug(message string, args ...interface{}) {
	util.MessageEntry(l.node, l.role).Debugf(message, args...)
}

func (l *roleLogger) Trace(message string, args ...interface{}) {
	util.MessageEntry(l.node, l.role).Tracef(message, args...)
}

func (l *roleLogger) Request(requestType string, request interface{}) {
	util.RequestEntry(l.node, l.role, requestType).
		Tracef("Received %v", request)
}

func (l *roleLogger) Response(responseType string, response interface{}, err error) error {
	util.ResponseEntry(l.node, l.role, responseType).
		Tracef("Sending %v", response)
	return err
}
