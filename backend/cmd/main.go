package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"vision-ward-terminal/backend/internal/orderbook"
	"vision-ward-terminal/backend/internal/ws"
)

func main() {

	hub := ws.NewHub()
	handler := ws.NewHandler(hub)

	go func() {
		hub.Run(context.Background())
	}()

	go func() {
		ch := orderbook.ManageOrderBookBTC(context.Background())
		for {
			data := <-ch
			dataBytes, err := json.Marshal(data)
			if err != nil {
				log.Println(err)
			}
			hub.Broadcast <- dataBytes
		}
	}()

	http.HandleFunc("/ws", handler.HandleWS)

	log.Fatal(http.ListenAndServe(":8080", nil))
}
