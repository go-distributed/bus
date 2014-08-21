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
)

// Config is used to store configs for the bus components.
type Config struct {
	Hostport     string
	SendRoutines int
	RecvRoutines int
}

// MakeDefaultConfig creates a default config, with the number
// of all routines set to 1.
func MakeDefaultConfig(hostport string) *Config {
	return &Config{
		Hostport:     hostport,
		SendRoutines: 1,
		RecvRoutines: 1,
	}
}

// String is implemented to print the config.
func (c *Config) String() string {
	return fmt.Sprintf("Config:\nHostport:%s\nSendRoutines:%d\nRecvRoutines:%d\n", c.Hostport, c.SendRoutines, c.RecvRoutines)
}
