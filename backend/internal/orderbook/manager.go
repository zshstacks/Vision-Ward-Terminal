package orderbook

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"sort"
	"strconv"
	"vision-ward-terminal/backend/internal/binance"
)

type OrderBookSnapshot struct {
	LastUpdateID int64      `json:"lastUpdateId"`
	Bids         [][]string `json:"bids"`
	Asks         [][]string `json:"asks"`
}

func ManageOrderBookBTC(ctx context.Context) <-chan OrderBookSnapshot {
	ch := make(chan OrderBookSnapshot)

	go func() {
		snapshot, err := fetchSnapshot()
		if err != nil {
			return
		}
		sortBids(snapshot.Bids)
		sortAsks(snapshot.Asks)

		orderbookBTC := binance.OrderbookBTC(ctx)

		for ctx.Err() == nil {
			orderbookBTCData := <-orderbookBTC
			if orderbookBTCData.Data.FinalUpdate <= snapshot.LastUpdateID {
				continue
			}

			//apply bid updates
			for _, upd := range orderbookBTCData.Data.Bids {
				price := upd[0]
				qty := upd[1]

				if qty == "0" {
					// remove price from snapshot.Bids if exists
					for i, b := range snapshot.Bids {
						if b[0] == price {
							snapshot.Bids = append(snapshot.Bids[:i], snapshot.Bids[i+1:]...)
							break
						}
					}
				} else {
					// update qty if exists, else append
					found := false
					for i, b := range snapshot.Bids {
						if b[0] == price {
							snapshot.Bids[i][1] = qty
							found = true
							break
						}
					}
					if !found {
						snapshot.Bids = append(snapshot.Bids, []string{price, qty})
					}
				}
			}

			//apply ask updates
			for _, upd := range orderbookBTCData.Data.Asks {
				price := upd[0]
				qty := upd[1]

				if qty == "0" {
					// remove price from snapshot.Asks if exists
					for i, a := range snapshot.Asks {
						if a[0] == price {
							snapshot.Asks = append(snapshot.Asks[:i], snapshot.Asks[i+1:]...)
							break
						}
					}
				} else {
					// update qty if exists, else append
					found := false
					for i, a := range snapshot.Asks {
						if a[0] == price {
							snapshot.Asks[i][1] = qty
							found = true
							break
						}
					}
					if !found {
						snapshot.Asks = append(snapshot.Asks, []string{price, qty})
					}
				}
			}
			snapshot.LastUpdateID = orderbookBTCData.Data.FinalUpdate

			sortBids(snapshot.Bids)
			sortAsks(snapshot.Asks)
			ch <- *snapshot
		}
	}()

	return ch
}

func fetchSnapshot() (*OrderBookSnapshot, error) {
	snapshotURL := "https://fapi.binance.com/fapi/v1/depth?symbol=BTCUSDT&limit=100"
	resp, err := http.Get(snapshotURL)
	if err != nil {
		log.Printf("Failed to fetch snapshot: %v", err)
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("Failed to read snapshot: %v", err)
		return nil, err
	}

	var snapshot OrderBookSnapshot
	if err := json.Unmarshal(body, &snapshot); err != nil {
		log.Printf("Failed to unmarshal snapshot: %v", err)
		return nil, err
	}
	log.Printf("got snapshot: lastUpdateId=%d bids=%d asks=%d", snapshot.LastUpdateID, len(snapshot.Bids), len(snapshot.Asks))
	return &snapshot, nil
}

// helpers: sort bids desc, asks asc
func sortBids(bids [][]string) {
	sort.Slice(bids, func(i, j int) bool {
		pi, _ := strconv.ParseFloat(bids[i][0], 64)
		pj, _ := strconv.ParseFloat(bids[j][0], 64)
		return pi > pj
	})
}

func sortAsks(asks [][]string) {
	sort.Slice(asks, func(i, j int) bool {
		pi, _ := strconv.ParseFloat(asks[i][0], 64)
		pj, _ := strconv.ParseFloat(asks[j][0], 64)
		return pi < pj
	})
}
