package shardctrler

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// import "time"

func check(t *testing.T, groups []int, ck *Clerk) {
	c := ck.Query(-1)

	if len(c.Groups) != len(groups) {
		t.Fatalf("Incorrect number of groups: expected %v, got %v in config: %+v", len(groups), len(c.Groups), c)
	}

	// Check if the groups are as expected
	for _, g := range groups {
		_, ok := c.Groups[g]
		if !ok {
			t.Fatalf("Missing expected group %v in config: %+v", g, c)
		}
	}

	// Check for any un-allocated shards
	if len(groups) > 0 {
		for s, g := range c.Shards {
			_, ok := c.Groups[g]
			if !ok {
				t.Fatalf("Shard %v assigned to invalid group %v in config: %+v", s, g, c)
			}
		}
	}

	// more or less balanced sharding?
	counts := map[int]int{}
	for _, g := range c.Shards {
		counts[g] += 1
	}
	min := 257
	max := 0
	for g, _ := range c.Groups {
		if counts[g] > max {
			max = counts[g]
		}
		if counts[g] < min {
			min = counts[g]
		}
	}
	if max > min+1 {
		t.Fatalf("Shard distribution imbalance: Max shards per group (%d) exceeds min shards per group (%d) by more than 1. Current configuration: %+v", max, min, c)
	}
}

func check_same_config(t *testing.T, c1 Config, c2 Config) {
	t.Logf("Comparing configurations:\n  c1: %+v\n  c2: %+v\n", c1, c2) // Log the configs

	if c1.Num != c2.Num {
		t.Fatalf("Configuration mismatch: Num in c1 (%d) differs from Num in c2 (%d)", c1.Num, c2.Num)
	}
	if c1.Shards != c2.Shards {
		t.Fatalf("Configuration mismatch: Shards in c1 (%v) differ from Shards in c2 (%v)", c1.Shards, c2.Shards)
	}
	if len(c1.Groups) != len(c2.Groups) {
		t.Fatalf("Configuration mismatch: Number of groups in c1 (%d) differs from c2 (%d)", len(c1.Groups), len(c2.Groups))
	}
	for gid, sa := range c1.Groups {
		sa1, ok := c2.Groups[gid]
		if !ok {
			t.Fatalf("Configuration mismatch: Group %d missing in c2", gid)
		}
		if len(sa1) != len(sa) {
			t.Fatalf("Configuration mismatch: Length of servers for group %d in c1 (%d) differs from c2 (%d)", gid, len(sa), len(sa1))
		}
		for j := 0; j < len(sa); j++ {
			if sa[j] != sa1[j] {
				t.Fatalf("Configuration mismatch: Servers for group %d differ at index %d: c1 has '%s', c2 has '%s'", gid, j, sa[j], sa1[j])
			}
		}
	}
}

func TestBasic(t *testing.T) {
	const nservers = 3
	cfg := make_config(t, nservers, false)
	defer cfg.cleanup()

	ck := cfg.makeClient(cfg.All())

	fmt.Printf("Test: Basic leave/join ...\n")

	cfa := make([]Config, 6)
	cfa[0] = ck.Query(-1)

	check(t, []int{}, ck)

	var gid1 int = 1
	ck.Join(map[int][]string{gid1: []string{"x", "y", "z"}})
	check(t, []int{gid1}, ck)
	cfa[1] = ck.Query(-1)

	var gid2 int = 2
	ck.Join(map[int][]string{gid2: []string{"a", "b", "c"}})
	check(t, []int{gid1, gid2}, ck)
	cfa[2] = ck.Query(-1)

	cfx := ck.Query(-1)
	sa1 := cfx.Groups[gid1]
	if len(sa1) != 3 || sa1[0] != "x" || sa1[1] != "y" || sa1[2] != "z" {
		t.Fatalf("wrong servers for gid %v: %v\n", gid1, sa1)
	}
	sa2 := cfx.Groups[gid2]
	if len(sa2) != 3 || sa2[0] != "a" || sa2[1] != "b" || sa2[2] != "c" {
		t.Fatalf("wrong servers for gid %v: %v\n", gid2, sa2)
	}

	ck.Leave([]int{gid1})
	check(t, []int{gid2}, ck)
	cfa[4] = ck.Query(-1)

	ck.Leave([]int{gid2})
	cfa[5] = ck.Query(-1)

	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: Historical queries ...\n")

	for s := 0; s < nservers; s++ {
		cfg.ShutdownServer(s)
		for i := 0; i < len(cfa); i++ {
			c := ck.Query(cfa[i].Num)
			check_same_config(t, c, cfa[i])
		}
		cfg.StartServer(s)
		cfg.ConnectAll()
	}

	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: Move ...\n")
	{
		var gid3 int = 503
		ck.Join(map[int][]string{gid3: []string{"3a", "3b", "3c"}})
		var gid4 int = 504
		ck.Join(map[int][]string{gid4: []string{"4a", "4b", "4c"}})
		for i := 0; i < NShards; i++ {
			cf := ck.Query(-1)
			if i < NShards/2 {
				ck.Move(i, gid3)
				if cf.Shards[i] != gid3 {
					cf1 := ck.Query(-1)
					if cf1.Num <= cf.Num {
						t.Fatalf("Move should increase Config.Num")
					}
				}
			} else {
				ck.Move(i, gid4)
				if cf.Shards[i] != gid4 {
					cf1 := ck.Query(-1)
					if cf1.Num <= cf.Num {
						t.Fatalf("Move should increase Config.Num")
					}
				}
			}
		}
		cf2 := ck.Query(-1)
		for i := 0; i < NShards; i++ {
			if i < NShards/2 {
				if cf2.Shards[i] != gid3 {
					t.Fatalf("expected shard %v on gid %v actually %v",
						i, gid3, cf2.Shards[i])
				}
			} else {
				if cf2.Shards[i] != gid4 {
					t.Fatalf("expected shard %v on gid %v actually %v",
						i, gid4, cf2.Shards[i])
				}
			}
		}
		ck.Leave([]int{gid3})
		ck.Leave([]int{gid4})
	}
	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: Concurrent leave/join ...\n")

	const npara = 10
	var cka [npara]*Clerk
	for i := 0; i < len(cka); i++ {
		cka[i] = cfg.makeClient(cfg.All())
	}
	gids := make([]int, npara)
	ch := make(chan bool)
	for xi := 0; xi < npara; xi++ {
		gids[xi] = int((xi * 10) + 100)
		go func(i int) {
			defer func() { ch <- true }()
			var gid int = gids[i]
			var sid1 = fmt.Sprintf("s%da", gid)
			var sid2 = fmt.Sprintf("s%db", gid)
			cka[i].Join(map[int][]string{gid + 1000: []string{sid1}})
			cka[i].Join(map[int][]string{gid: []string{sid2}})
			cka[i].Leave([]int{gid + 1000})
		}(xi)
	}
	for i := 0; i < npara; i++ {
		<-ch
	}
	check(t, gids, ck)

	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: Minimal transfers after joins ...\n")

	c1 := ck.Query(-1)
	for i := 0; i < 5; i++ {
		var gid = int(npara + 1 + i)
		ck.Join(map[int][]string{gid: []string{
			fmt.Sprintf("%da", gid),
			fmt.Sprintf("%db", gid),
			fmt.Sprintf("%db", gid)}})
	}
	c2 := ck.Query(-1)
	for i := int(1); i <= npara; i++ {
		for j := 0; j < len(c1.Shards); j++ {
			if c2.Shards[j] == i {
				if c1.Shards[j] != i {
					t.Fatalf("non-minimal transfer after Join()s")
				}
			}
		}
	}

	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: Minimal transfers after leaves ...\n")

	for i := 0; i < 5; i++ {
		ck.Leave([]int{int(npara + 1 + i)})
	}
	c3 := ck.Query(-1)
	for i := int(1); i <= npara; i++ {
		for j := 0; j < len(c1.Shards); j++ {
			if c2.Shards[j] == i {
				if c3.Shards[j] != i {
					t.Fatalf("non-minimal transfer after Leave()s")
				}
			}
		}
	}

	fmt.Printf("  ... Passed\n")
}

func TestMulti(t *testing.T) {
	const nservers = 3
	cfg := make_config(t, nservers, false)
	defer cfg.cleanup()

	ck := cfg.makeClient(cfg.All())

	fmt.Printf("Test: Multi-group join/leave ...\n")

	cfa := make([]Config, 6)
	cfa[0] = ck.Query(-1)

	check(t, []int{}, ck)

	var gid1 int = 1
	var gid2 int = 2
	ck.Join(map[int][]string{
		gid1: []string{"x", "y", "z"},
		gid2: []string{"a", "b", "c"},
	})
	check(t, []int{gid1, gid2}, ck)
	cfa[1] = ck.Query(-1)

	var gid3 int = 3
	ck.Join(map[int][]string{gid3: []string{"j", "k", "l"}})
	check(t, []int{gid1, gid2, gid3}, ck)
	cfa[2] = ck.Query(-1)

	cfx := ck.Query(-1)
	sa1 := cfx.Groups[gid1]
	if len(sa1) != 3 || sa1[0] != "x" || sa1[1] != "y" || sa1[2] != "z" {
		t.Fatalf("wrong servers for gid %v: %v\n", gid1, sa1)
	}
	sa2 := cfx.Groups[gid2]
	if len(sa2) != 3 || sa2[0] != "a" || sa2[1] != "b" || sa2[2] != "c" {
		t.Fatalf("wrong servers for gid %v: %v\n", gid2, sa2)
	}
	sa3 := cfx.Groups[gid3]
	if len(sa3) != 3 || sa3[0] != "j" || sa3[1] != "k" || sa3[2] != "l" {
		t.Fatalf("wrong servers for gid %v: %v\n", gid3, sa3)
	}

	ck.Leave([]int{gid1, gid3})
	check(t, []int{gid2}, ck)
	cfa[3] = ck.Query(-1)

	cfx = ck.Query(-1)
	sa2 = cfx.Groups[gid2]
	if len(sa2) != 3 || sa2[0] != "a" || sa2[1] != "b" || sa2[2] != "c" {
		t.Fatalf("wrong servers for gid %v: %v\n", gid2, sa2)
	}

	ck.Leave([]int{gid2})

	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: Concurrent multi leave/join ...\n")

	const npara = 10
	var cka [npara]*Clerk
	for i := 0; i < len(cka); i++ {
		cka[i] = cfg.makeClient(cfg.All())
	}
	gids := make([]int, npara)
	var wg sync.WaitGroup
	for xi := 0; xi < npara; xi++ {
		wg.Add(1)
		gids[xi] = int(xi + 1000)
		go func(i int) {
			defer wg.Done()
			var gid int = gids[i]
			cka[i].Join(map[int][]string{
				gid: []string{
					fmt.Sprintf("%da", gid),
					fmt.Sprintf("%db", gid),
					fmt.Sprintf("%dc", gid)},
				gid + 1000: []string{fmt.Sprintf("%da", gid+1000)},
				gid + 2000: []string{fmt.Sprintf("%da", gid+2000)},
			})
			cka[i].Leave([]int{gid + 1000, gid + 2000})
		}(xi)
	}
	wg.Wait()
	check(t, gids, ck)

	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: Minimal transfers after multijoins ...\n")

	c1 := ck.Query(-1)
	m := make(map[int][]string)
	for i := 0; i < 5; i++ {
		var gid = npara + 1 + i
		m[gid] = []string{fmt.Sprintf("%da", gid), fmt.Sprintf("%db", gid)}
	}
	ck.Join(m)
	c2 := ck.Query(-1)
	for i := int(1); i <= npara; i++ {
		for j := 0; j < len(c1.Shards); j++ {
			if c2.Shards[j] == i {
				if c1.Shards[j] != i {
					t.Fatalf("non-minimal transfer after Join()s")
				}
			}
		}
	}

	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: Minimal transfers after multileaves ...\n")

	var l []int
	for i := 0; i < 5; i++ {
		l = append(l, npara+1+i)
	}
	ck.Leave(l)
	c3 := ck.Query(-1)
	for i := int(1); i <= npara; i++ {
		for j := 0; j < len(c1.Shards); j++ {
			if c2.Shards[j] == i {
				if c3.Shards[j] != i {
					t.Fatalf("non-minimal transfer after Leave()s")
				}
			}
		}
	}

	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: Check Same config on servers ...\n")

	isLeader, leader := cfg.Leader()
	if !isLeader {
		t.Fatalf("Leader not found")
	}
	c := ck.Query(-1) // Config leader claims

	cfg.ShutdownServer(leader)

	attempts := 0
	for isLeader, leader = cfg.Leader(); isLeader; time.Sleep(1 * time.Second) {
		if attempts++; attempts >= 3 {
			t.Fatalf("Leader not found")
		}
	}

	c1 = ck.Query(-1)
	check_same_config(t, c, c1)

	fmt.Printf("  ... Passed\n")
}
