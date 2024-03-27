package main

import (
	"log"
	"net/rpc"
)

type Item struct {
	title string
}

func main() {
	var reply Item
	var db []Item

	println("Connecting to the client....")
	client, err := rpc.DialHTTP("tcp", "localhost:4040")

	if err != nil {
		log.Fatal("Connection error: ", err)
	}

	println("Client connected!")

	a := Item{"FirstItem"}
	b := Item{"SecondItem"}
	c := Item{"ThirdItem"}

	client.Call("API.AddItem", a, &reply)
	client.Call("API.AddItem", b, &reply)
	client.Call("API.GetByNameRPC", b, &reply)
	client.Call("API.GetDB", c, &db)

	println("Done calling!")

	println("Database:", db)
}
