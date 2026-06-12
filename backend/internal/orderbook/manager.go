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
	LastUpdateID int64               `json:"lastUpdateId"`
	Bids         map[float64]float64 `json:"bids"` //O(1)
	Asks         map[float64]float64 `json:"asks"`
}

type SortedOrderBookSnapshot struct {
	LastUpdateID int64      `json:"lastUpdateId"`
	Bids         [][]string `json:"bids"`
	Asks         [][]string `json:"asks"`
}

type rawSnapshot struct {
	LastUpdateID int64      `json:"lastUpdateId"`
	Bids         [][]string `json:"bids"`
	Asks         [][]string `json:"asks"`
}

func ManageOrderBookBTC(ctx context.Context) <-chan SortedOrderBookSnapshot {
	ch := make(chan SortedOrderBookSnapshot)

	go func() {
		orderbookBTC := binance.OrderbookBTC(ctx)

		snapshot, err := fetchSnapshot(ctx)
		if err != nil {
			return
		}

		synced := false

	outer:
		for {
			select {
			case <-ctx.Done():
				return
			case msg := <-orderbookBTC:
				//correct Binance algorithm:
				//Start WebSocket connection first
				//Then fetch the REST snapshot
				//Check anchor on incoming messages
				if !synced {
					if msg.Data.FirstUpdate <= snapshot.LastUpdateID+1 && msg.Data.FinalUpdate >= snapshot.LastUpdateID+1 {
						synced = true
					} else if msg.Data.FinalUpdate < snapshot.LastUpdateID+1 {
						continue
					} else if msg.Data.FirstUpdate > snapshot.LastUpdateID+1 {
						result, err := fetchSnapshot(ctx)
						if err != nil {
							log.Println(err)
							return
						}
						snapshot = result
						continue
					}
				} else {

					if msg.Data.FinalUpdateLastStream != snapshot.LastUpdateID {
						log.Printf("gap detected: pu=%d snapshot=%d", msg.Data.FinalUpdateLastStream, snapshot.LastUpdateID)
						log.Printf("gap detected, restarting")
						synced = false
						result, err := fetchSnapshot(ctx)
						if err != nil {
							log.Println(err)
							return
						}
						snapshot = result
						continue outer
					}
				}

				//apply bid updates
				for _, upd := range msg.Data.Bids {
					price, _ := strconv.ParseFloat(upd[0], 64)
					qty, _ := strconv.ParseFloat(upd[1], 64)

					if qty == 0 {
						// remove price from snapshot.Bids if exists
						delete(snapshot.Bids, price)
					} else {
						// update qty if exists, else append
						snapshot.Bids[price] = qty
					}
				}

				//apply ask updates
				for _, upd := range msg.Data.Asks {
					price, _ := strconv.ParseFloat(upd[0], 64)
					qty, _ := strconv.ParseFloat(upd[1], 64)

					if qty == 0 {
						// remove price from snapshot.Asks if exists
						delete(snapshot.Asks, price)
					} else {
						// update qty if exists, else append
						snapshot.Asks[price] = qty
					}
				}
				snapshot.LastUpdateID = msg.Data.FinalUpdate

				ch <- SortedOrderBookSnapshot{
					LastUpdateID: snapshot.LastUpdateID,
					Bids:         sortBids(snapshot.Bids),
					Asks:         sortAsks(snapshot.Asks),
				}
			}
		}
	}()

	return ch
}

func fetchSnapshot(ctx context.Context) (*OrderBookSnapshot, error) {
	snapshotURL := "https://fapi.binance.com/fapi/v1/depth?symbol=BTCUSDT&limit=100"

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, snapshotURL, nil)
	if err != nil {
		log.Printf("Failed to fetch snapshot: %v", err)
		return nil, err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Printf("Client execution err: %v", err)
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("Failed to read snapshot: %v", err)
		return nil, err
	}

	var rawSnapshot rawSnapshot
	var snapshot OrderBookSnapshot
	if err := json.Unmarshal(body, &rawSnapshot); err != nil {
		log.Printf("Failed to unmarshal snapshot: %v", err)
		return nil, err
	}

	snapshot.LastUpdateID = rawSnapshot.LastUpdateID
	snapshot.Bids = make(map[float64]float64)
	snapshot.Asks = make(map[float64]float64)

	for _, upd := range rawSnapshot.Bids {
		price, _ := strconv.ParseFloat(upd[0], 64)
		qty, _ := strconv.ParseFloat(upd[1], 64)

		if qty == 0 {
			// remove price from snapshot.Bids if exists
			delete(snapshot.Bids, price)
		} else {
			// update qty if exists, else append
			snapshot.Bids[price] = qty
		}
	}

	for _, upd := range rawSnapshot.Asks {
		price, _ := strconv.ParseFloat(upd[0], 64)
		qty, _ := strconv.ParseFloat(upd[1], 64)

		if qty == 0 {
			// remove price from snapshot.Asks if exists
			delete(snapshot.Asks, price)
		} else {
			// update qty if exists, else append
			snapshot.Asks[price] = qty
		}
	}

	log.Printf("got snapshot: lastUpdateId=%d bids=%d asks=%d", snapshot.LastUpdateID, len(snapshot.Bids), len(snapshot.Asks))
	return &snapshot, nil
}

// helpers: sort bids desc, asks asc
func sortBids(bids map[float64]float64) [][]string {
	keys := make([]float64, 0, len(bids))
	for k := range bids {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i] > keys[j]
	})
	result := make([][]string, 0, len(keys))
	for _, k := range keys {
		result = append(result, []string{strconv.FormatFloat(k, 'f', -1, 64), strconv.FormatFloat(bids[k], 'f', -1, 64)})
	}
	return result
}

func sortAsks(asks map[float64]float64) [][]string {
	keys := make([]float64, 0, len(asks))
	for k := range asks {
		keys = append(keys, k)
	}

	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})
	result := make([][]string, 0, len(keys))
	for _, k := range keys {
		result = append(result, []string{strconv.FormatFloat(k, 'f', -1, 64), strconv.FormatFloat(asks[k], 'f', -1, 64)})

	}
	return result
}
