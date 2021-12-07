# HeavyKeeper

This package implements the HeavyKeeper algorithm for efficiently finding top-K
flows in an unbounded set of flows.

Paper: https://www.usenix.org/system/files/conference/atc18/atc18-gong.pdf

The [reference implementation][reference] is pretty difficult to follow. I found
the [RedisBloom implementation][redisbloom] to be far eaier to read, if you're
interested in seeing a more battle-tested implementation. The RedisBloom
implementation also has some optimizations that might be worth looking at (a
decay lookup table, for example) and it supports arbitrary increments greater
than one, which I've implemented here.

[reference]: https://github.com/papergitkeeper/heavy-keeper-project/blob/master/heavykeeper.h
[redisbloom]: https://github.com/RedisBloom/RedisBloom/blob/master/src/topk.c

This implementation uses a default width and depth to simplify usage:

* `width = k * log(k)` (minimum of 256)
* `height = log(k)` (minimum of 3)

## Usage

```go
hk := topk.New(100, 0.9)

hk.Add("foo", 1)
hk.Add("bar", 5)
hk.Add("baz", 1)
hk.Add("baz", 1)

for _, fc := range hk.Top() {
  fmt.Printf("%s = %d\n", fc.Flow, fc.Count)
}

// bar = 5
// baz = 2
// foo = 1

count, ok := hk.Count("bar")
fmt.Println(count, ok)
// 5 true
```

## Benchmarks

The algorithm itself is rather efficient on its own; I haven't invested any time
in further optimizing things (yet).

```
goos: darwin
goarch: amd64
pkg: github.com/segmentio/topk
cpu: Intel(R) Core(TM) i7-9750H CPU @ 2.60GHz
BenchmarkAdd/K=10-12         	17065675	        79.38 ns/op	       0 B/op	       0 allocs/op
BenchmarkAdd/K=50-12         	11193319	       106.5 ns/op	       0 B/op	       0 allocs/op
BenchmarkAdd/K=100-12        	 9880362	       131.8 ns/op	       0 B/op	       0 allocs/op
BenchmarkAdd/K=500-12        	 7442464	       159.6 ns/op	       0 B/op	       0 allocs/op
BenchmarkAdd/K=1000-12       	 7125268	       167.8 ns/op	       0 B/op	       0 allocs/op
BenchmarkAdd/K=5000-12       	 5797017	       206.3 ns/op	       0 B/op	       0 allocs/op
BenchmarkAdd/K=10000-12      	 5218218	       233.2 ns/op	       0 B/op	       0 allocs/op
```

