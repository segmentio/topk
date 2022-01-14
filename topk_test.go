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
	t.Helper()
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
				hk.Sample(fc.Flow, fc.Count)
			}
			assert(t, test.expect, hk.Top())
		})
	}
}

func TestSample_returnValue(t *testing.T) {
	hk := New(2, 0.9)

	assert := func(key string, expect bool) {
		t.Helper()
		got := hk.Sample(key, 1)
		if got != expect {
			t.Fatalf("for key %s, expected %v, got %v", key, expect, got)
		}
	}

	assert("a", true)
	assert("a", true)
	assert("a", true)

	assert("b", true)
	assert("b", true)

	assert("c", false)
	assert("c", true)
}

func TestDecayAll(t *testing.T) {
	hk := New(5, 0.9)
	hk.Sample("a", 3)
	hk.Sample("b", 6)
	hk.Sample("c", 13)
	hk.Sample("d", 25)
	hk.Sample("e", 50)
	hk.Sample("f", 100)

	hk.DecayAll(0.3)
	assert(t, []FlowCount{{"f", 70}, {"e", 35}, {"d", 17}, {"c", 9}, {"b", 4}}, hk.Top())

	hk.DecayAll(0.9)
	hk.DecayAll(0.9)
	assert(t, []FlowCount{}, hk.Top())
}

func TestReset(t *testing.T) {
	hk := New(5, 0.9)
	hk.Sample("a", 1)
	hk.Sample("b", 2)
	hk.Sample("c", 3)
	hk.Reset()
	assert(t, []FlowCount{}, hk.Top())
}

func BenchmarkSample(b *testing.B) {
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
				hk.Sample(flow, 1)
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
