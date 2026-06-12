package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
	"vision-ward-terminal/backend/internal/orderbook"
	"vision-ward-terminal/backend/internal/ws"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	hub := ws.NewHub()
	handler := ws.NewHandler(hub)

	go func() {
		hub.Run(ctx)
	}()

	go func() {
		ch := orderbook.ManageOrderBookBTC(ctx)
		for {
			select {
			case data := <-ch:
				dataBytes, err := json.Marshal(data)
				if err != nil {
					log.Println(err)
				}
				select {
				case hub.Broadcast <- dataBytes:
				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				return
			}

		}
	}()

	http.HandleFunc("/ws", handler.HandleWS)

	server := http.Server{
		Addr:    ":8080",
		Handler: http.DefaultServeMux,
	}

	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %s\n", err)
		}

	}()

	<-ctx.Done()

	ctxServer, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	server.Shutdown(ctxServer)

}
