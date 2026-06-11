package ws

import (
	"context"

	"github.com/coder/websocket"
)

// Hub maintains the set of active clients and broadcasts messages to the clients
type Hub struct {
	// Registered clients
	clients map[*websocket.Conn]bool
	// Inbound messages from the clients
	register chan *websocket.Conn
	// Register requests from the clients
	unregister chan *websocket.Conn
	// Unregister requests from clients
	Broadcast chan []byte
}

func NewHub() *Hub {
	return &Hub{
		Broadcast:  make(chan []byte),
		register:   make(chan *websocket.Conn),
		unregister: make(chan *websocket.Conn),
		clients:    make(map[*websocket.Conn]bool),
	}
}

func (h *Hub) Run(ctx context.Context) {
	for {
		select {
		case client := <-h.register:
			h.clients[client] = true
		case client := <-h.unregister:
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)

			}
		case message := <-h.Broadcast:
			for client := range h.clients {
				err := client.Write(ctx, websocket.MessageText, message)
				if err != nil {
					delete(h.clients, client)
					client.CloseNow()
				}
			}
		case <-ctx.Done():
			for client := range h.clients {
				client.CloseNow()
			}
			return
		}
	}
}
