package binance

import (
	"context"
	"log"
	"net/url"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

type DepthUpdateMessage struct {
	Data struct {
		EventType             string      `json:"e"`
		EventTime             int64       `json:"E"`
		TransactionTime       int64       `json:"T"`
		Symbol                string      `json:"s"`
		FirstUpdate           int64       `json:"U"`
		FinalUpdate           int64       `json:"u"`
		FinalUpdateLastStream int64       `json:"pu"`
		Bids                  [][2]string `json:"b"` //[]slice [2]pair of strings
		Asks                  [][2]string `json:"a"`
	} `json:"data"`
}

func OrderbookBTC(ctx context.Context) <-chan DepthUpdateMessage {
	ch := make(chan DepthUpdateMessage)
	u := url.URL{
		Scheme:   "wss",
		Host:     "fstream.binance.com",
		Path:     "/stream",
		RawQuery: "streams=btcusdt@depth20@100ms",
	}

	log.Printf("connecting to %s", u.String())

	go func() {
		c, _, err := websocket.Dial(ctx, u.String(), nil)
		if err != nil {
			log.Printf("dial: %v", err)
			return
		}
		defer c.Close(websocket.StatusNormalClosure, "")

		for ctx.Err() == nil {
			btc := &DepthUpdateMessage{}
			err := wsjson.Read(ctx, c, btc)
			if err != nil {
				log.Printf("json read: %v", err)
				break
			}
			ch <- *btc
		}

	}()

	return ch
}
