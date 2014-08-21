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

package bus

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

// BusInterface defines the interface of a bus.
type BusInterface interface {
	InstallMessage(msg proto.Message, handler MessageHandler) error
	InstallEvent(event *Event, handler EventHandler) error
	Send(to string, msg proto.Message)
	Trigger(event *Event)
	Shutdown()
}

// Bus implements the BusInterface.
type Bus struct {
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

// NewBus creates and starts a new bus using the giving config.
// It also creates and starts the underlying transporter.
func NewBus(config *Config) *Bus {
	b := &Bus{
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
	b.tr = tr
	go b.encodeLoop()
	go b.decodeLoop()
	go b.eventLoop()
	return b
}

// InstallMessage installs the message with the giving message handler.
// It returns an error if the message is already installed or the message is invalid.
func (b *Bus) InstallMessage(msg proto.Message, handler MessageHandler) error {
	rtype := reflect.TypeOf(msg)
	if rtype.Kind() != reflect.Ptr {
		err := fmt.Errorf("Message %v is not a Ptr type", rtype)
		log.Errorf("Failed to install message: %v\n", err)
		return err
	}
	name := rtype.Elem().Name()
	if _, ok := b.messageNameToMtype[name]; ok {
		err := fmt.Errorf("Message %v is already installed", name)
		log.Errorf("Failed to install message: %v\n", err)
		return err
	}
	mtype := uint8(len(b.messageNameToMtype))
	b.messageNameToMtype[name] = mtype
	b.messageType[mtype] = rtype
	b.messageHandler[mtype] = handler
	return nil
}

// InstallEvent installs the event with the giving handler.
// It returns an error if the event is already installed.
func (b *Bus) InstallEvent(event *Event, handler EventHandler) error {
	if _, ok := b.eventHandler[event.Etype]; ok {
		err := fmt.Errorf("Event %v is already installed", event.Etype)
		log.Errorf("Failed to install event: %v\n", err)
		return err
	}
	b.eventHandler[event.Etype] = handler
	return nil
}

// Send puts a message in the sendQueue.
func (b *Bus) Send(to string, msg proto.Message) {
	defer func() {
		if err := recover(); err != nil {
			log.Errorf("Failed to send: %v\n", err)
		}
	}()
	name := reflect.TypeOf(msg).Elem().Name()
	mtype, ok := b.messageNameToMtype[name]
	if !ok {
		log.Errorf("Failed to send: unknown message: %v\n", name)
		return
	}
	b.sendQueue <- &Message{Mtype: mtype, Addr: to, Msg: msg}
}

// Trigger puts an event in the eventQueue.
func (b *Bus) Trigger(event *Event) {
	b.eventQueue <- event
}

// Shutdown shuts down the bus. It will stop the underlying transporter first.
func (b *Bus) Shutdown() {
	b.tr.Shutdown()
	close(b.stop)
}

// eventLoop runs forever until the bus is shutdown. It will try to
// receive messages or events and dispatch them.
func (b *Bus) eventLoop() {
	for {
		select {
		case <-b.stop:
			return
		case msg := <-b.recvQueue:
			b.dispatchMessage(msg)
		case event := <-b.eventQueue:
			b.dispatchEvent(event)
		}
	}
}

// dispatchMessage calls the installed handler to handle the message.
func (b *Bus) dispatchMessage(msg *Message) {
	b.messageHandler[msg.Mtype](msg.Addr, msg.Msg)
}

// dispatchEvent calls the installed handler to handle the event.
func (b *Bus) dispatchEvent(event *Event) {
	handler, ok := b.eventHandler[event.Etype]
	if !ok {
		log.Warningf("Uninstalled event: %v\n", event.Etype)
		return
	}
	handler(event)
}

// encodeLoop runs forever until the bus is shutdown.
func (b *Bus) encodeLoop() {
	for {
		select {
		case <-b.stop:
			return
		case msg := <-b.sendQueue:
			if err := b.encode(msg); err != nil {
				continue
			}
			b.tr.Send(msg)
		}
	}
}

// decodeLoop runs forever until the bus is shutdown.
func (b *Bus) decodeLoop() {
	for {
		select {
		case <-b.stop:
			return
		default:
		}
		msg := b.tr.Recv()
		if err := b.decode(msg); err != nil {
			continue
		}
		b.recvQueue <- msg
	}
}

// encode encodes the message into bytes and append the message's type
// at the end of the payload. This is necessary because protobuf is not
// self-explained.
func (b *Bus) encode(msg *Message) error {
	bs, err := proto.Marshal(msg.Msg)
	if err != nil {
		log.Warningf("Failed to marshal message[%v]: %v\n", msg.Mtype, err)
		return err
	}
	msg.Payload = append(bs, byte(msg.Mtype))
	return nil
}

// decode decodes the message from the bytes. It will get the type of the
// message first, then use reflect to create a message, and do a proto.Unmarshal().
func (b *Bus) decode(msg *Message) error {
	mtype := uint8(msg.Payload[len(msg.Payload)-1])
	rtype, ok := b.messageType[mtype]
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
