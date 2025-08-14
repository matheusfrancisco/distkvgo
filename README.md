# Dist KV POC

This is a PoC using boltdb to store key-value pairs in a distributed manner.

Using sharding, we can distribute the data across multiple nodes. 
Each node will be responsible for a subset of the keys.


### How to run:

```bash
> go run . -path=/Users/chico/m/projects/diskvgo/A.db -shard=A
2025/08/13 21:12:04 Shard count is 3, current shard: 0

> go run . -path=/Users/chico/m/projects/diskvgo/B.db -shard=B
2025/08/13 21:12:04 Shard count is 3, current shard: 1

> go run . -path=/Users/chico/m/projects/diskvgo/C.db -shard=C
2025/08/13 21:12:04 Shard count is 3, current shard: 1

```
