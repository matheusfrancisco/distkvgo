package main

import (
	"flag"
	"log"
	"net/http"

	"github.com/matheusfrancisco/diskvgo/config"
	"github.com/matheusfrancisco/diskvgo/db"
	"github.com/matheusfrancisco/diskvgo/replicate"
	"github.com/matheusfrancisco/diskvgo/server"
)

var (
	dbLocation = flag.String("path", "", "the path to the bolt db database")
	httpAddr  = flag.String("http-addr", "127.0.0.1:8080", "Http server address to listen on")
	configFile = flag.String("confile", "shard.toml", "Config file for static sharding")
	shard      = flag.String("shard", "", "The name of the shard for the data")
	replica    = flag.Bool("replica", false, "Whether or not run as a read-only replica")
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
	c, err := config.ParseFile(*configFile)
	if err != nil {
		log.Fatalf("Error parsing config file %q: %v", *configFile, err)
	}
	shards, err := config.ParseShards(c.Shards, *shard)
	if err != nil {
		log.Fatalf("Error parsing shards from config file %q: %v", *configFile, err)
	}

	log.Printf("Shard count is %d, current shard: %d", shards.Count, shards.CurIdx)

	d, err := db.New(*dbLocation, *replica)
	if err != nil {
		log.Fatalf("Error creating %q: %v", *dbLocation, err)
	}
	defer d.Close()
	if *replica {
		leaderAddr, ok := shards.Addrs[shards.CurIdx]
		if !ok {
			log.Fatalf("Current shard %d is not found in the config file %q", shards.CurIdx, *configFile)
		}
		go replicate.StartReplication(d, leaderAddr)
	}

	srv := server.New(d, shards)

	http.HandleFunc("/get", srv.GetHandler)
	http.HandleFunc("/set", srv.SetHandler)
	http.HandleFunc("/purge", srv.DeleteReshardKeysHandler)
	http.HandleFunc("/next-replication-key", srv.GetNextKeyForReplicationHandler)
	http.HandleFunc("/delete-replication-key", srv.DeleteReplicationKeyHandler)



	log.Fatal(http.ListenAndServe(*httpAddr, nil))
}
