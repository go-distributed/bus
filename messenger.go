/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package actor

import (
	"fmt"
	"reflect"

	"code.google.com/p/gogoprotobuf/proto"
	log "github.com/golang/glog"
)

// ActorMessenger defines the interface of a messenger.
type ActorMessenger interface {
	Install(mtype uint8, msg proto.Message)
	Send(msg *Message)
	Recv() *Message
	Shutdown()
}

// Messenger implements the ActorMessenger.
type Messenger struct {
	config            *Config
	tr                Transporter
	stop              chan struct{}
	installedMessages map[uint8]reflect.Type

	sendQueue chan *Message
	recvQueue chan *Message
}

// NewMessenger creates and starts a new messenger using the giving config.
// It also creates and starts the underlying transporter.
func NewMessenger(config *Config) *Messenger {
	m := &Messenger{
		config:            config,
		stop:              make(chan struct{}),
		installedMessages: make(map[uint8]reflect.Type),
		sendQueue:         make(chan *Message),
		recvQueue:         make(chan *Message),
	}
	tr := NewUDPTransporter(config)
	if tr == nil {
		log.Errorf("Failed to start transporter\n")
		return nil
	}
	m.tr = tr
	for i := 0; i < config.EncodeRoutines; i++ {
		go m.encodeLoop()
	}
	for i := 0; i < config.DecodeRoutines; i++ {
		go m.decodeLoop()
	}
	return m
}

// Install installs the message with the giving mtype. It returns an error
// if the mtype is alreadly been used, or the message is invalid.
func (m *Messenger) Install(mtype uint8, msg proto.Message) error {
	rtype, ok := m.installedMessages[mtype]
	if ok {
		err := fmt.Errorf("Message[%v] already installed at key = %v", rtype, mtype)
		log.Warningf("Failed to install message: %v\n", err)
		return err
	}
	rtype = reflect.TypeOf(msg)
	if rtype.Kind() != reflect.Ptr {
		err := fmt.Errorf("Message %v is not a Ptr type")
		log.Warningf("Failed to install message: %v\n", err)
		return err
	}
	m.installedMessages[mtype] = rtype.Elem()
	return nil
}

// Shutdown shuts down the messenger. It will stop the underlying transporter first.
func (m *Messenger) Shutdown() {
	m.tr.Shutdown()
	close(m.stop)
}

// Send puts a message in the sendQueue.
func (m *Messenger) Send(msg *Message) {
	m.sendQueue <- msg
}

// Recv tries to get a message from the recvQueue.
func (m *Messenger) Recv() *Message {
	return <-m.recvQueue
}

// encodeLoop runs forever until the messenger is shutdown.
func (m *Messenger) encodeLoop() {
	for {
		select {
		case <-m.stop:
			return
		case msg := <-m.sendQueue:
			if err := m.encode(msg); err != nil {
				continue
			}
			m.tr.Send(msg)
		}
	}
}

// decodeLoop runs forever until the messenger is shutdown.
func (m *Messenger) decodeLoop() {
	for {
		select {
		case <-m.stop:
			return
		default:
		}
		msg := m.tr.Recv()
		if err := m.decode(msg); err != nil {
			continue
		}
		m.recvQueue <- msg
	}
}

// encode encodes the message into bytes and append the message's type
// at the end of the payload. This is necessary because protobuf is not
// self-explained.
func (m *Messenger) encode(msg *Message) error {
	rtype, ok := m.installedMessages[msg.Mtype]
	if !ok {
		err := fmt.Errorf("Unknown message type: %v", msg.Mtype)
		log.Warningf("Failed to marshal message: %v\n", err)
		return err
	}
	b, err := proto.Marshal(msg.Msg)
	if err != nil {
		log.Warningf("Failed to marshal message[%v]: %v\n", rtype, err)
		return err
	}
	msg.Payload = append(b, byte(msg.Mtype))
	return nil
}

// decode decodes the message from the bytes. It will get the type of the
// message first, then use reflect to create a message, and do a proto.Unmarshal().
func (m *Messenger) decode(msg *Message) error {
	mtype := uint8(msg.Payload[len(msg.Payload)-1])
	rtype, ok := m.installedMessages[mtype]
	if !ok {
		err := fmt.Errorf("Unknown message type: %v", mtype)
		log.Warningf("Failed to unmarshal message[payload=%v]: %v\n", msg.Payload, err)
		return err
	}
	msg.Msg = reflect.New(rtype).Interface().(proto.Message)
	if err := proto.Unmarshal(msg.Payload[0:len(msg.Payload)-1], msg.Msg); err != nil {
		log.Warningf("Failed to unmarshal message[%v]: %v\n", rtype, err)
		return err
	}
	return nil
}
