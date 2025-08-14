package config

import (
	"fmt"
	"hash/fnv"

	"github.com/BurntSushi/toml"
)

type Shard struct {
	Name    string
	Idx     int
	Address string
}

type Config struct {
	Shards []Shard
}

type Shards struct {
	Count  int
	CurIdx int
	Addrs  map[int]string
}

func ParseFile(filename string) (*Config, error) {
	var conf Config
	if _, err := toml.DecodeFile(filename, &conf); err != nil {
		return nil, err
	}
	return &conf, nil
}

func ParseShards(shards []Shard, curShardName string) (*Shards, error) {
	shardCount := len(shards)
	shardIdx := -1
	addrs := make(map[int]string)

	for _, s := range shards {
		if _, ok := addrs[s.Idx]; ok {
			return nil, fmt.Errorf("duplicate shard index: %d", s.Idx)
		}

		addrs[s.Idx] = s.Address
		if s.Name == curShardName {
			shardIdx = s.Idx
		}
	}

	for i := 0; i < shardCount; i++ {
		if _, ok := addrs[i]; !ok {
			return nil, fmt.Errorf("shard %d is not found", i)
		}
	}

	if shardIdx < 0 {
		return nil, fmt.Errorf("shard %q was not found", curShardName)
	}

	return &Shards{
		Addrs:  addrs,
		Count:  shardCount,
		CurIdx: shardIdx,
	}, nil
}

func (s *Shards) Index(key string) int {
	// Index returns the shard number for the corresponding key.
	h := fnv.New64()
	h.Write([]byte(key))
	return int(h.Sum64() % uint64(s.Count))
}
