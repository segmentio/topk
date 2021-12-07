package topk

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"
)

func format(flowCounts []FlowCount) string {
	var buf bytes.Buffer
	buf.WriteRune('[')
	for i, fc := range flowCounts {
		if i > 0 {
			buf.WriteString(", ")
		}
		fmt.Fprintf(&buf, "%s=%d", fc.Flow, fc.Count)
	}
	buf.WriteRune(']')
	return buf.String()
}

func assert(t *testing.T, expect, given []FlowCount) {
	if len(expect) != len(given) {
		t.Fatalf("expected %d items, got %d:\nexpect: %s\ngot:    %s", len(expect), len(given), format(expect), format(given))
	} else {
		for i := range expect {
			if expect[i] != given[i] {
				t.Fatalf("element at %d doesn't match:\nexpect: %s\ngot:    %s", i, format(expect), format(given))
			}
		}
	}
}

func TestHeavyKeeper(t *testing.T) {
	type flowCount struct {
		flow  string
		count uint32
	}

	tests := []struct {
		desc   string
		k      int
		given  []FlowCount
		expect []FlowCount
	}{
		{
			desc:   "zero",
			k:      5,
			expect: []FlowCount{},
		},
		{
			desc: "simple, cardinality < k",
			k:    5,
			given: []FlowCount{
				{"c", 1},
				{"b", 5},
				{"a", 10},
				{"d", 25},
			},
			expect: []FlowCount{
				{"d", 25},
				{"a", 10},
				{"b", 5},
				{"c", 1},
			},
		},
		{
			desc: "simple, cardinality > k",
			k:    5,
			given: []FlowCount{
				{"c", 1},
				{"b", 5},
				{"a", 10},
				{"d", 25},
				{"f", 3},
				{"g", 20},
				{"h", 100},
				{"i", 2},
			},
			expect: []FlowCount{
				{"h", 100},
				{"d", 25},
				{"g", 20},
				{"a", 10},
				{"b", 5},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			hk := New(test.k, 0.9)
			for _, fc := range test.given {
				hk.Add(fc.Flow, fc.Count)
			}
			assert(t, test.expect, hk.Top())
		})
	}
}

func TestReset(t *testing.T) {
	hk := New(5, 0.9)
	hk.Add("a", 1)
	hk.Add("b", 2)
	hk.Add("c", 3)
	hk.Reset()
	assert(t, []FlowCount{}, hk.Top())
}

func BenchmarkAdd(b *testing.B) {
	flows := make([]string, 1_000_000)
	for i := range flows {
		flows[i] = randString(24)
	}

	for _, k := range []int{10, 50, 100, 500, 1_000, 5_000, 10_000} {
		b.Run(fmt.Sprintf("K=%d", k), func(b *testing.B) {
			if len(flows) < b.N {
				for i := len(flows); i <= b.N; i++ {
					flows = append(flows, randString(16))
				}
			}
			flows := make([]string, b.N)
			hk := New(k, 0.9)
			b.ResetTimer()
			for _, flow := range flows[:b.N] {
				hk.Add(flow, 1)
			}
		})
	}
}

const chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func randString(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = chars[rand.Int63()%int64(len(chars))]
	}
	return string(b)
}
