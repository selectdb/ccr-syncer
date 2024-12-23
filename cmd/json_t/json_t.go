// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License
package main

import (
	"encoding/json"
	"fmt"
)

type JsonT struct {
	Id    int         `json:"id"`
	IdP   *int        `json:"id_p"`
	Value interface{} `json:"value"`
}

type inMemory struct {
	Uid  int    `json:"uid"`
	Name string `json:"name"`
}

type Map struct {
	Ids map[int64]int64 `json:"ids"`
}

func main() {
	// ids := make(map[int64]int64)
	idsMap := Map{}
	idsData, err := json.Marshal(&idsMap)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s\n", string(idsData))

	inMemoryV := inMemory{
		Uid:  1,
		Name: "test",
	}

	idP := 10
	jsonT := JsonT{
		Id:    1,
		IdP:   &idP,
		Value: &inMemoryV,
	}

	data, err := json.Marshal(&jsonT)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s\n", string(data))

	// j2 := JsonT{}
	// if err := json.Unmarshal(data, &j2); err != nil {
	// 	panic(err)
	// }
	// inMemory2 := j2.Value.(*inMemory)
	// fmt.Printf("%+v\n", inMemory2)
}
