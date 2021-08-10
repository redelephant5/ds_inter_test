package main

import (
	"ds_inter_test/lib/heartbeat"
	"ds_inter_test/locate"
	"ds_inter_test/objects"
	"ds_inter_test/temp"
	"ds_inter_test/versions"
	"log"
	"net/http"
	"os"
)

func main(){
	go heartbeat.ListenHeartbeat()
	http.HandleFunc("/objects/", objects.Handler)
	http.HandleFunc("/locate/", locate.Handler)
	http.HandleFunc("/versions/", versions.Handler)
	http.HandleFunc("/temp/", temp.Handler)
	log.Fatal(http.ListenAndServe(os.Getenv("LISTEN_ADDRESS"), nil))

}

