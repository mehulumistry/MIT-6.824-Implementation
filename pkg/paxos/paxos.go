package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (decided bool, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import (
	"math"
	"net"
)
import "net/rpc"
import "log"
import "os"
import "syscall"
import "sync"
import "fmt"
import "math/rand"

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       bool
	unreliable bool
	rpcCount   int
	peers      []string
	me         int // index into peers[]

	// Your data here.
	max         int // number returned by Max()
	instances   map[int]*Instance
	doneSeq     []int // Map of peerId and seq
	proposalNum int   // Highest proposal number used by this node
}

type Instance struct {
	n_p int // Highest prepare seen

	n_a int         // Highest accept seen
	v_a interface{} // Value of the highest accept seen

	decided bool        // Whether the instance has been decided
	v_d     interface{} // Decided value
}

type PrepareArgs struct {
	Seq    int
	N      int
	FromId int
}

type AcceptArgs struct {
	Seq    int
	N      int
	V      interface{}
	FromId int
}

type DecideArgs struct {
	Seq    int
	V      interface{}
	Done   int
	FromId int
}

type PrepareReply struct {
	Ok  bool
	N_a int
	V_a interface{}
}

type AcceptReply struct {
	Ok bool
}

type DecideReply struct {
	Ok   bool
	Done int
}

// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
func (px *Paxos) Start(seq int, v interface{}) {
	px.mu.Lock()
	if seq > px.max {
		px.max = seq
	}
	px.mu.Unlock()

	go px.proposer(seq, v)
}

// use function call if srv == me, otherwise use RPC call()
// also calls freememory
func (px *Paxos) Call2(srv string, name string, args interface{}, reply interface{}) bool {
	var err error

	if srv == px.peers[px.me] {
		if name == "Paxos.Prepare" {
			prepareArgs := args.(PrepareArgs)
			prepareReply := reply.(*PrepareReply)
			err = px.Prepare(&prepareArgs, prepareReply)
		} else if name == "Paxos.Accept" {
			acceptArgs := args.(AcceptArgs)
			acceptReply := reply.(*AcceptReply)
			err = px.Accept(&acceptArgs, acceptReply)
		} else if name == "Paxos.Decide" {
			decidedArgs := args.(DecideArgs)
			decidedReply := reply.(*DecideReply)
			err = px.Decide(&decidedArgs, decidedReply)
		} else {
			return false
		}
		if err == nil {
			return true
		}

		fmt.Println(err)
		return false
	} else {
		result := call(srv, name, args, reply)
		return result
	}
}

func (px *Paxos) proposer(seq int, v interface{}) {
	inst := px.GetInstance(seq)
	for !inst.decided && !px.dead {
		px.mu.Lock()
		// Increment the highest proposal number
		px.proposalNum++
		n := (px.proposalNum << 16) | px.me // Combine sequence number with node ID
		px.mu.Unlock()

		log.Printf("[PREPARE][Node: %d] starting proposal for seq %d with proposal number %d", px.me, seq, n)

		// Send prepare(n) to all servers
		prepareArgs := PrepareArgs{Seq: seq, N: n, FromId: px.me}
		prepareReplies := make(chan PrepareReply, len(px.peers))
		for i, peer := range px.peers {
			go func(peer string, i int) {
				var reply PrepareReply
				ok := px.Call2(peer, "Paxos.Prepare", prepareArgs, &reply)
				if ok {
					prepareReplies <- reply
				} else {
					prepareReplies <- PrepareReply{Ok: false}
				}
				log.Printf("[PREPARE][Node: %d][RPC] received PrepareReply from [Node: %d]: %+v", px.me, i, reply)
			}(peer, i)
		}

		// Wait for the majority of prepare_ok
		okCount := 0      // Starting with 1 to count own vote
		var highestNa int // highest proposal number from response --> take that and send accepts
		var vPrime interface{}
		for i := 0; i < len(px.peers); i++ {
			reply := <-prepareReplies
			if reply.Ok {
				okCount++
				if reply.N_a > highestNa {
					highestNa = reply.N_a
					vPrime = reply.V_a
				}
			}
		}

		log.Printf("[PREPARE][Node: %d] received %d for seq %d", px.me, okCount, seq)

		if okCount < len(px.peers)/2+1 {
			log.Printf("[PREPARE][Node: %d] did not receive majority for prepare_ok, retrying...", px.me)
			continue // Retry with a new proposal number
		}

		// this means no reply has higher value that args.
		if vPrime == nil {
			vPrime = v
		}

		log.Printf("[ACCEPT][Node: %d] moving to accept phase with value %v for seq %d", px.me, vPrime, seq)

		// Send accept(n, v') to all servers
		acceptArgs := AcceptArgs{Seq: seq, N: n, V: vPrime, FromId: px.me}
		acceptReplies := make(chan AcceptReply, len(px.peers))

		for indx, peer := range px.peers {
			go func(peer string, indx int) {
				var reply AcceptReply
				ok := px.Call2(peer, "Paxos.Accept", acceptArgs, &reply)
				if ok {
					acceptReplies <- reply
				} else {
					acceptReplies <- AcceptReply{Ok: false}
				}
				log.Printf("[ACCEPT][Node: %d][RPC] received AcceptReply from [Node: %d]: %+v", px.me, indx, reply)
			}(peer, indx)
		}

		// Wait for the majority of accept_ok
		okCount = 0 // Starting with 1 to count own vote
		for i := 0; i < len(px.peers); i++ {
			reply := <-acceptReplies
			if reply.Ok {
				okCount++
			}
		}

		log.Printf("[ACCEPT][Node: %d] received %d for seq %d", px.me, okCount, seq)

		if okCount >= len(px.peers)/2+1 {
			// Send decided(v') to all servers
			decideArgs := DecideArgs{Seq: seq, V: vPrime, Done: px.doneSeq[px.me], FromId: px.me}
			for index, peer := range px.peers {
				func(peer string, indx int) {
					var reply DecideReply
					ok := px.Call2(peer, "Paxos.Decide", decideArgs, &reply)
					if ok {
						px.MergeDoneVals(reply.Done, indx)
						log.Printf("[DECIDE][Node: %d][RPC] sent Decide message to [Node: %d] for seq %d", px.me, indx, seq)
					}
				}(peer, index)
			}

			log.Printf("[DECIDE][Node: %d] received %d for seq %d", px.me, okCount, seq)

		} else {
			log.Printf("[DECIDE][Node: %d] did not receive majority for accept_ok, retrying...", px.me)
		}
	}
}
func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
	if args.Seq < px.Min() {
		reply.Ok = false
		return nil
	}

	instance := px.GetInstance(args.Seq)

	if args.N > instance.n_p {
		instance.n_p = args.N
		reply.Ok = true
		reply.N_a = instance.n_a
		reply.V_a = instance.v_a
		log.Printf("[PREPARE][Node: %d][From: %d] accepted prepare request with n=%d for seq %d", px.me, args.FromId, args.N, args.Seq)
	} else {
		reply.Ok = false
		log.Printf("[PREPARE][Node: %d][From: %d] rejected prepare request with n=%d for seq %d, instance: %+v", px.me, args.FromId, args.N, args.Seq, instance)
	}

	return nil
}
func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {

	if args.Seq < px.Min() {
		return nil
	}

	instance := px.GetInstance(args.Seq)

	if args.N >= instance.n_p {
		instance.n_p = args.N
		instance.n_a = args.N
		instance.v_a = args.V
		reply.Ok = true
		log.Printf("[ACCEPT][Node: %d][From: %d] accepted accept request with n=%d for seq %d", px.me, args.FromId, args.N, args.Seq)
	} else {
		reply.Ok = false
		log.Printf("[ACCEPT][Node: %d][From: %d] rejected accept request with n=%d for seq %d", px.me, args.FromId, args.N, args.Seq)
	}

	return nil
}

func (px *Paxos) MergeDoneVals(doneVal int, from int) {
	px.mu.Lock()
	defer px.mu.Unlock()

	if doneVal > px.doneSeq[from] {
		px.doneSeq[from] = doneVal
	}
}

func (px *Paxos) Decide(args *DecideArgs, reply *DecideReply) error {
	px.MergeDoneVals(args.Done, args.FromId)
	reply.Done = px.doneSeq[px.me]

	if args.Seq < px.Min() {
		return nil
	}

	instance := px.GetInstance(args.Seq)

	instance.decided = true
	instance.v_d = args.V
	reply.Ok = true
	log.Printf("[DECIDE][Node: %d][From: %d] decided value %v for seq %d", px.me, args.FromId, args.V, args.Seq)
	return nil
}

// returns px.instances[seq], creating it if necessary
func (px *Paxos) GetInstance(seq int) *Instance {
	px.mu.Lock()
	defer px.mu.Unlock()

	_, ok := px.instances[seq]
	if !ok {
		px.instances[seq] = MakeInstance()
	}
	return px.instances[seq]
}

func MakeInstance() *Instance {
	inst := &Instance{}
	inst.decided = false
	inst.v_a = nil
	return inst
}

// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
func (px *Paxos) Done(seq int) {
	px.mu.Lock()
	if seq > px.doneSeq[px.me] {
		px.doneSeq[px.me] = seq
	}
	px.mu.Unlock()

	px.Min() //this will force a free memory
	//log.Printf("Done: Node %d done with seq %d, DONE SEQ: %+v", px.me, seq, px.doneSeq[px.me])
}

// the application wants to know the
// highest instance sequence known to
// this peer.
func (px *Paxos) Max() int {
	px.mu.Lock()
	defer px.mu.Unlock()
	log.Printf("Max: Node %d maximum sequence is %d", px.me, px.max)
	return px.max
}

// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
func (px *Paxos) Min() int {
	px.mu.Lock()
	minDoneVal := math.MaxInt32
	for i := 0; i < len(px.peers); i++ {
		//log.Printf("Node %d: doneSeq[%d] = %d", px.me, i, px.doneSeq[i])
		if px.doneSeq[i] < minDoneVal {
			minDoneVal = px.doneSeq[i]
		}
	}
	px.mu.Unlock()

	retVal := 1 + minDoneVal
	//log.Printf("Node %d: Calculated Min() = %d", px.me, retVal)

	px.FreeMemory(retVal)
	//log.Printf("Node %d: Called FreeMemory(%d)", px.me, retVal)

	return retVal
}

// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
func (px *Paxos) Status(seq int) (bool, interface{}) {
	px.mu.Lock()
	defer px.mu.Unlock()

	instance, ok := px.instances[seq]
	if ok && instance.decided {
		log.Printf("[Status][Node: %d][SEQ: %d] is decided with value %v", px.me, seq, instance.v_d)
		return true, instance.v_d
	}
	log.Printf("[Status][Node: %d][SEQ: %d] is not decided", px.me, seq)
	return false, nil
}

func (px *Paxos) FreeMemory(keepAtLeast int) {
	for i := 0; i < keepAtLeast; i++ {
		px.mu.Lock()
		delete(px.instances, i)
		px.mu.Unlock()
	}
}

// tell the peer to shut itself down.
// for testing.
// please do not change this function.
func (px *Paxos) Kill() {
	px.dead = true
	if px.l != nil {
		px.l.Close()
	}
}

// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me
	px.max = -1
	// Your initialization code here.
	// Your initialization code here.
	px.instances = make(map[int]*Instance)
	px.doneSeq = make([]int, len(peers))
	px.proposalNum = 0
	for i := range px.peers {
		px.doneSeq[i] = -1
	}

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		//
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.dead == false {
				conn, err := px.l.Accept()
				if err == nil && px.dead == false {
					if px.unreliable && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.unreliable && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						px.rpcCount++
						go rpcs.ServeConn(conn)
					} else {
						px.rpcCount++
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.dead == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}
