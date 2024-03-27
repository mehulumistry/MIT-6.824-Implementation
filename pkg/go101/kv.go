package main

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"
)

//
// Common RPC request/reply definitions
//

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
)

type Err string

type PutArgs struct {
	Key   string
	Value string
}

type PutReply struct {
	Err Err
}

type GetArgs struct {
	Key string
}

type GetReply struct {
	Err   Err
	Value string
}

//
// Client
//

func connect() *rpc.Client {
	client, err := rpc.Dial("tcp", ":1234")
	if err != nil {
		log.Fatal("dialing:", err)
	}
	return client
}

func get(key string) string {
	client := connect()
	args := GetArgs{"subject"}
	reply := GetReply{}
	err := client.Call("KV.Get", &args, &reply)
	if err != nil {
		log.Fatal("error:", err)
	}
	client.Close()
	return reply.Value
}

func put(key string, val string) {
	client := connect()
	args := PutArgs{"subject", "6.824"}
	reply := PutReply{}
	err := client.Call("KV.Put", &args, &reply)
	if err != nil {
		log.Fatal("error:", err)
	}
	client.Close()
}

//
// Server
//

type KV struct {
	mu   sync.Mutex
	data map[string]string
}

func server() {
	kv := new(KV)
	kv.data = map[string]string{}
	rpcs := rpc.NewServer()
	err := rpcs.Register(kv)

	if err != nil {
		return
	}

	l, e := net.Listen("tcp", ":1234")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go func() {
		for {
			conn, err := l.Accept()
			if err == nil {
				go rpcs.ServeConn(conn)
			} else {
				break
			}
		}
		err := l.Close()
		if err != nil {
			return
		}
	}()
}

func (kv *KV) Get(args *GetArgs, reply *GetReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	val, ok := kv.data[args.Key]
	if ok {
		reply.Err = OK
		reply.Value = val
	} else {
		reply.Err = ErrNoKey
		reply.Value = ""
	}
	return nil
}

func (kv *KV) Put(args *PutArgs, reply *PutReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.data[args.Key] = args.Value
	reply.Err = OK
	return nil
}

//
// main
//

func main() {
	//server()
	//
	//put("subject", "6.824")
	//fmt.Printf("Put(subject, 6.824) done\n")
	//fmt.Printf("get(subject) -> %s\n", get("subject"))

	/*
		- All the functions needs to be method not function.
		- second param must be pointer and is result which we return to the client
		- Return type must be error. If return type is error then it will not send any data to client.

	*/

	var api = new(API)
	_ = rpc.Register(api)

	rpc.HandleHTTP()
	// To open connection
	listener, _ := net.Listen("tcp", ":4040")
	_ = http.Serve(listener, nil)

}

type Item struct {
	title string
}

type API int

var database []Item

func GetByName(title string) Item {
	var getItem Item

	for _, val := range database {
		if val.title == title {
			getItem = val
		}
	}
	return getItem
}

func (a *API) GetDB(title string, reply *[]Item) error {
	*reply = database
	return nil
}

func (a *API) GetByNameRPC(title string, reply *Item) error {
	var getItem Item

	for _, val := range database {
		if val.title == title {
			getItem = val
		}
	}
	*reply = getItem

	return nil
}

func (a *API) AddItem(item Item, reply *Item) error {
	database = append(database, item)
	*reply = item
	return nil
}
