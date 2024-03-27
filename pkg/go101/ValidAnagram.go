package main

import (
	"sort"
	"strings"
)

func IsAnagram(s string, t string) bool {

	ss := strings.Split(s, "")
	ts := strings.Split(t, "")

	sort.Strings(ss)
	sort.Strings(ts)

	for i := 0; i < max(len(ss), len(ts)); i++ {

		if len(ss) <= i || len(ts) <= i {
			return false
		}

		same := ss[i] == ts[i]
		if same {
			continue
		} else {
			return false
		}
	}

	return true

}
