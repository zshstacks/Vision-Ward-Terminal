package ws

import (
	"log"
	"net/http"
	"vision-ward-terminal/backend/internal/binance"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

func HandleWS(w http.ResponseWriter, r *http.Request) {

	c, err := websocket.Accept(w, r, nil)
	if err != nil {
		log.Printf("Accept ws Error: %v", err)
		return
	}
	defer func(c *websocket.Conn) {
		err := c.CloseNow()
		if err != nil {
			log.Printf("CloseNow ws Error: %v", err)
		}
	}(c)

	ctx := r.Context()
	orderbookBTC := binance.OrderbookBTC(ctx)

	for ctx.Err() == nil {
		orderbookBTCData := <-orderbookBTC
		err := wsjson.Write(ctx, c, orderbookBTCData)
		if err != nil {
			log.Printf("Write ws Error: %v", err)
			break
		}

	}

}
