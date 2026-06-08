package main

import (
	"log"
	"net/http"
	"vision-ward-terminal/backend/internal/ws"
)

func main() {
	http.HandleFunc("/ws", ws.HandleWS)

	log.Fatal(http.ListenAndServe(":8080", nil))
}
