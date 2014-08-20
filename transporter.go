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
)

type Transporter interface {
	Stop()
	Send(msg *Message) error
	Recv() *Message
}

type TransporterConfig struct {
	Hostport     string
	SendRoutines int
	RecvRoutines int
}

func MakeDefaultTransporterConfig(hostport string) *TransporterConfig {
	return &TransporterConfig{
		Hostport:     hostport,
		SendRoutines: 1,
		RecvRoutines: 1,
	}
}

func (tc *TransporterConfig) String() {
	fmt.Sprintf("TransporterConfig:\nHostport:%s\nSendRoutines:%d\nRecvRoutines:%d\n", tc.Hostport, tc.SendRoutines, tc.RecvRoutines)
}
