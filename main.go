package main

import (
	"flag"
	"log"
	"net/http"

	"github.com/BurntSushi/toml"
	"github.com/matheusfrancisco/diskvgo/db"
	"github.com/matheusfrancisco/diskvgo/server"
)

type Shard struct {
	Name    string
	Idx     int
	Address string
}

type Config struct {
	Shards []Shard
}

var (
	dbLocation = flag.String("path", "", "the path to the bolt db database")
	httpAddrs  = flag.String("http-addr", "127.0.0.1:8080", "Http server address to listen on")
	configFile = flag.String("confile", "shard.toml", "Config file for static sharding")
	shard      = flag.String("shard", "", "The name of the shard for the data")
)

func parseFlags() {
	flag.Parse()
	if *dbLocation == "" {
		log.Fatal("Please provide a path to the bolt db database using the -path flag")
	}

	if *shard == "" {
		log.Fatal("Please provide a shard name using the -shard flag")
	}
}

func main() {
	parseFlags()
	var conf Config
	if _, err := toml.DecodeFile(*configFile, &conf); err != nil {
		log.Fatalf("Error reading config file %s: %v", *configFile, err)
	}

	var sCount int
	var shardIdx int = -1
	var addrs = make(map[int]string)
	for _, s := range conf.Shards {
		addrs[s.Idx] = s.Address

		if s.Idx+1 > sCount {
			sCount = s.Idx + 1
		}

		if s.Name == *shard {
			shardIdx = s.Idx
		}
	}

	if shardIdx < 0 {
		log.Fatalf("Shard %q was not found", *shard)
	}
	log.Printf("Shard count is %d, current shard: %d", sCount, shardIdx)

	db, err := db.New(*dbLocation)

	if err != nil {
		log.Fatalf("Creating NewDB error(%q): %v", *dbLocation, err)
	}
	defer db.Close()
	s := server.New(db, shardIdx, sCount, addrs)
	http.HandleFunc("/get", s.GetHandler)
	http.HandleFunc("/set", s.SetHandler)

	log.Fatal(http.ListenAndServe(*httpAddrs, nil))
}
