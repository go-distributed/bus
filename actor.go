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

// MessageHandler is a callback to handle incoming messages.
// 'from' indicates the sender of the message, 'message' is the protobuf
// message sent by the sender.
type MessageHandler func(from string, message proto.Message)

// EventHandler is a callback to handle events. The only arguments it
// takes is the event itself.
type EventHandler func(event *Event)

// ActorInterface defines the interface of a messenger.
type ActorInterface interface {
	InstallMessage(msg proto.Message, handler MessageHandler) error
	InstallEvent(event *Event, handler EventHandler) error
	Send(to string, msg proto.Message)
	Trigger(event *Event)
	Shutdown()
}

// Actor implements the ActorInterface.
type Actor struct {
	config *Config
	tr     Transporter
	stop   chan struct{}

	messageNameToMtype map[string]uint8
	messageType        map[uint8]reflect.Type
	messageHandler     map[uint8]MessageHandler
	eventHandler       map[uint8]EventHandler

	sendQueue  chan *Message
	recvQueue  chan *Message
	eventQueue chan *Event
}

// NewActor creates and starts a new messenger using the giving config.
// It also creates and starts the underlying transporter.
func NewActor(config *Config) *Actor {
	a := &Actor{
		config:             config,
		stop:               make(chan struct{}),
		messageNameToMtype: make(map[string]uint8),
		messageType:        make(map[uint8]reflect.Type),
		messageHandler:     make(map[uint8]MessageHandler),
		eventHandler:       make(map[uint8]EventHandler),
		sendQueue:          make(chan *Message, defaultQueueSize),
		recvQueue:          make(chan *Message, defaultQueueSize),
		eventQueue:         make(chan *Event, defaultQueueSize),
	}
	tr := NewUDPTransporter(config)
	if tr == nil {
		log.Errorf("Failed to start transporter\n")
		return nil
	}
	a.tr = tr
	go a.encodeLoop()
	go a.decodeLoop()
	go a.eventLoop()
	return a
}

// InstallMessage installs the message with the giving message handler.
// It returns an error if the message is already installed or the message is invalid.
func (a *Actor) InstallMessage(msg proto.Message, handler MessageHandler) error {
	rtype := reflect.TypeOf(msg)
	if rtype.Kind() != reflect.Ptr {
		err := fmt.Errorf("Message %v is not a Ptr type", rtype)
		log.Errorf("Failed to install message: %v\n", err)
		return err
	}
	name := rtype.Elem().Name()
	if _, ok := a.messageNameToMtype[name]; ok {
		err := fmt.Errorf("Message %v is already installed", name)
		log.Errorf("Failed to install message: %v\n", err)
		return err
	}
	mtype := uint8(len(a.messageNameToMtype))
	a.messageNameToMtype[name] = mtype
	a.messageType[mtype] = rtype
	a.messageHandler[mtype] = handler
	return nil
}

// InstallEvent installs the event with the giving handler.
// It returns an error if the event is already installed.
func (a *Actor) InstallEvent(event *Event, handler EventHandler) error {
	if _, ok := a.eventHandler[event.Etype]; ok {
		err := fmt.Errorf("Event %v is already installed", event.Etype)
		log.Errorf("Failed to install event: %v\n", err)
		return err
	}
	a.eventHandler[event.Etype] = handler
	return nil
}

// Send puts a message in the sendQueue.
func (a *Actor) Send(to string, msg proto.Message) {
	defer func() {
		if err := recover(); err != nil {
			log.Errorf("Failed to send: %v\n", err)
		}
	}()
	name := reflect.TypeOf(msg).Elem().Name()
	mtype, ok := a.messageNameToMtype[name]
	if !ok {
		log.Errorf("Failed to send: unknown message: %v\n", name)
		return
	}
	a.sendQueue <- &Message{Mtype: mtype, Addr: to, Msg: msg}
}

// Trigger puts an event in the eventQueue.
func (a *Actor) Trigger(event *Event) {
	a.eventQueue <- event
}

// Shutdown shuts down the messenger. It will stop the underlying transporter first.
func (a *Actor) Shutdown() {
	a.tr.Shutdown()
	close(a.stop)
}

// eventLoop runs forever until the actor is shutdown. It will try to
// receive messages or events and dispatch them.
func (a *Actor) eventLoop() {
	for {
		select {
		case <-a.stop:
			return
		case msg := <-a.recvQueue:
			a.dispatchMessage(msg)
		case event := <-a.eventQueue:
			a.dispatchEvent(event)
		}
	}
}

// dispatchMessage calls the installed handler to handle the message.
func (a *Actor) dispatchMessage(msg *Message) {
	a.messageHandler[msg.Mtype](msg.Addr, msg.Msg)
}

// dispatchEvent calls the installed handler to handle the event.
func (a *Actor) dispatchEvent(event *Event) {
	handler, ok := a.eventHandler[event.Etype]
	if !ok {
		log.Warningf("Uninstalled event: %v\n", event.Etype)
		return
	}
	handler(event)
}

// encodeLoop runs forever until the messenger is shutdown.
func (a *Actor) encodeLoop() {
	for {
		select {
		case <-a.stop:
			return
		case msg := <-a.sendQueue:
			if err := a.encode(msg); err != nil {
				continue
			}
			a.tr.Send(msg)
		}
	}
}

// decodeLoop runs forever until the messenger is shutdown.
func (a *Actor) decodeLoop() {
	for {
		select {
		case <-a.stop:
			return
		default:
		}
		msg := a.tr.Recv()
		if err := a.decode(msg); err != nil {
			continue
		}
		a.recvQueue <- msg
	}
}

// encode encodes the message into bytes and append the message's type
// at the end of the payload. This is necessary because protobuf is not
// self-explained.
func (a *Actor) encode(msg *Message) error {
	b, err := proto.Marshal(msg.Msg)
	if err != nil {
		log.Warningf("Failed to marshal message[%v]: %v\n", msg.Mtype, err)
		return err
	}
	msg.Payload = append(b, byte(msg.Mtype))
	return nil
}

// decode decodes the message from the bytes. It will get the type of the
// message first, then use reflect to create a message, and do a proto.Unmarshal().
func (a *Actor) decode(msg *Message) error {
	mtype := uint8(msg.Payload[len(msg.Payload)-1])
	rtype, ok := a.messageType[mtype]
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
