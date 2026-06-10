package ws

import (
	"log"
	"net/http"

	"github.com/coder/websocket"
)

type Handler struct {
	hub *Hub
}

func (h *Handler) HandleWS(w http.ResponseWriter, r *http.Request) {

	c, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		InsecureSkipVerify: true,
	})
	if err != nil {
		log.Printf("Accept ws Error: %v", err)
		return
	}

	h.hub.register <- c
	<-r.Context().Done() //Wait until the client disconnects
	h.hub.unregister <- c
}

func NewHandler(hub *Hub) *Handler {
	return &Handler{
		hub: hub,
	}
}
