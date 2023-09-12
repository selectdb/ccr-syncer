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
