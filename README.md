# Dist KV POC

This is a PoC using boltdb to store key-value pairs in a distributed manner.

Using sharding, we can distribute the data across multiple nodes. 
Each node will be responsible for a subset of the keys.

#### For sharding the keys:

when we add a new shard we need to rehash the keys to distribute them evenly across the shards.
so if we use a number of power of 2 shards, we can use the last bit of the hash to determine the shard.
for exemple we have 2 shards and when we add 4 shards what will happen is that the keys will be rehashed and distributed across the new shards.
and this will be almost half of the keys from 0 will move to 2 and half of the keys from 1 
will be moved to 3. (0, 1, 2, 3) will have almost the same number of keys.

The problem is if you have 1024 shards and you want to add more, for each 
you will have to add a new shard then you will have 2048 shards and you will have to rehash all the keys again.

There are some solutions:
1. Range-based sharding: 
   - You can use ranges of keys to determine which shard they belong to. 
   - This way, you can add new shards without rehashing all the keys.

2. Consistent hashing:
    - This is a more complex solution, but it allows you to add new shards without rehashing all the keys.
    - It uses a hash ring to determine which shard a key belongs to.

3. Geography-based sharding:
    - This is a more complex solution, but it allows you to add new shards without rehashing all the keys.
    - It uses the geography of the data to determine which shard a key belongs to.


For our PoC, we will use the first solution, power of two sharding.

### How to run:

```bash
> go run . -path=/Users/chico/m/projects/diskvgo/A.db -shard=A
2025/08/13 21:12:04 Shard count is 3, current shard: 0

> go run . -path=/Users/chico/m/projects/diskvgo/B.db -shard=B
2025/08/13 21:12:04 Shard count is 3, current shard: 1

> go run . -path=/Users/chico/m/projects/diskvgo/C.db -shard=C
2025/08/13 21:12:04 Shard count is 3, current shard: 1

```

