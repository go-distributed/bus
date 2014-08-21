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
	"net"
	"sync"

	log "github.com/golang/glog"
)

// UDPTransporter implements the Transporter interfaces using
// UDP communication.
type UDPTransporter struct {
	config *Config

	addr *net.UDPAddr
	conn *net.UDPConn

	sendQueue chan *Message
	recvQueue chan *Message

	stop chan struct{}
	pool *sync.Pool
}

// NewUDPTransporter creates and starts the transporter
func NewUDPTransporter(config *Config) *UDPTransporter {
	addr, err := net.ResolveUDPAddr("tcp", config.Hostport)
	if err != nil {
		log.Errorf("Failed to resolve UDP address: %v\n", err)
		return nil
	}
	conn, err := net.ListenUDP("tcp", addr)
	if err != nil {
		log.Errorf("Failed to listen UDP: %v\n", err)
		return nil
	}
	tr := &UDPTransporter{
		config:    config,
		addr:      addr,
		conn:      conn,
		sendQueue: make(chan *Message, defaultQueueSize),
		recvQueue: make(chan *Message, defaultQueueSize),
		stop:      make(chan struct{}),
		pool:      &sync.Pool{New: makeUDPBufferFunc},
	}
	for i := 0; i < config.SendRoutines; i++ {
		go tr.sendLoop()
	}
	for i := 0; i < config.RecvRoutines; i++ {
		go tr.recvLoop()
	}
	return tr
}

// makeUDPBufferFunc creates empty UDP payload buffer.
// It is used by sync.Pool.
func makeUDPBufferFunc() interface{} {
	return make([]byte, 64*1024)
}

// sendLoop runs forever until the transporter is shutdown.
func (tr *UDPTransporter) sendLoop() {
	for {
		select {
		case <-tr.stop:
			return
		case msg := <-tr.sendQueue:
			tr.writeUDP(msg)
		}
	}
}

// recvLoop runs forever until the transporter is shutdown.
func (tr *UDPTransporter) recvLoop() {
	for {
		select {
		case <-tr.stop:
			return
		default:
		}
		tr.readUDP()
	}
}

// writeUDP sends a single UDP packet, using the message's Payload as
// the packet's payload.
func (tr *UDPTransporter) writeUDP(msg *Message) {
	addr, err := net.ResolveUDPAddr("udp", msg.Addr)
	if err != nil {
		log.Warningf("Failed to resolve UDP address: %v\n", err)
		return
	}
	n, err := tr.conn.WriteToUDP(msg.Payload, addr)
	if err != nil {
		log.Warningf("Failed to write to UDP: %v\n", err)
	}
	if n != len(msg.Payload) {
		log.Warningf("Partitial write: %d of %d bytes\n", n, len(msg.Payload))
	}
}

// readUDP read a single UDP packet, and fill the message's Payload with
// the bytes it reads.
func (tr *UDPTransporter) readUDP() {
	b := tr.pool.Get().([]byte)
	defer tr.pool.Put(b)
	n, addr, err := tr.conn.ReadFromUDP(b)
	if err != nil {
		log.Warningf("Failed to read from UDP: %v\n", err)
		return
	}
	msg := &Message{
		Addr:    addr.String(),
		Payload: make([]byte, n),
	}
	if nn := copy(msg.Payload, b[0:n]); nn != n {
		log.Warningf("Partitial copy: %d of %d bytes\n", nn, n)
	}
}

// Send puts a message in the sendQueue.
func (tr *UDPTransporter) Send(msg *Message) {
	tr.sendQueue <- msg
}

// Recv tries to get a message from the recvQueue, it will block if
// there is no messages available now.
func (tr *UDPTransporter) Recv() *Message {
	return <-tr.recvQueue
}

// Shutdown shuts down the transporter.
func (tr *UDPTransporter) Shutdown() {
	tr.conn.Close()
	close(tr.stop)
}
