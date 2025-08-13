package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"

	"github.com/matheusfrancisco/diskvgo/db"
)

var (
	dbLocation = flag.String("path", "", "the path to the bolt db database")
	httpAddrs  = flag.String("http-addr", "127.0.0.1:8080", "Http server address to listen on")
)

func parseFlags() {
	flag.Parse()
	if *dbLocation == "" {
		log.Fatal("Please provide a path to the bolt db database using the -path flag")
	}
}

func main() {
	parseFlags()
	db,err := db.New(*dbLocation)

	if err != nil {
		log.Fatalf("Creating NewDB error(%q): %v", *dbLocation, err)
	}
	defer db.Close()

	http.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
		r.ParseForm()

		key := r.FormValue("key")
		value, err := db.GetKey(key)
		if err != nil {
			http.Error(w, fmt.Sprintf("Error getting key %s: %v", key, err), http.StatusInternalServerError)
			return
		}
		fmt.Fprintf(w, "Key: %s, Value: %s\n", key, value)
	})

	http.HandleFunc("/set", func(w http.ResponseWriter, r *http.Request) {
		r.ParseForm()

		key := r.FormValue("key")
		value := r.FormValue("value")
		if key == "" || value == "" {
			http.Error(w, "Key and value must be provided", http.StatusBadRequest)
			return
		}

		err := db.SetKey(key, []byte(value))
		if err != nil {
			http.Error(w, fmt.Sprintf("Error setting key %s: %v", key, err), http.StatusInternalServerError)
			return
		}
		fmt.Fprintf(w, "Key %s set successfully\n", key)
	})

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "OK")
	})

	log.Fatal(http.ListenAndServe(*httpAddrs, nil))
}
