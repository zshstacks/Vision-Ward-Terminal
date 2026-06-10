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
	LastUpdateID int64             `json:"lastUpdateId"`
	Bids         map[string]string `json:"bids"` //O(1)
	Asks         map[string]string `json:"asks"`
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
		snapshot, err := fetchSnapshot()
		if err != nil {
			return
		}

		orderbookBTC := binance.OrderbookBTC(ctx)

		for ctx.Err() == nil {

			orderbookBTCData := <-orderbookBTC

			if orderbookBTCData.Data.FinalUpdate <= snapshot.LastUpdateID {
				continue
			}

			//if !synced {
			//	if orderbookBTCData.Data.FirstUpdate <= snapshot.LastUpdateID && orderbookBTCData.Data.FinalUpdate >= snapshot.LastUpdateID {
			//		synced = true
			//	} else {
			//		if orderbookBTCData.Data.FirstUpdate > snapshot.LastUpdateID {
			//			result, err := fetchSnapshot()
			//			if err != nil {
			//				log.Println(err)
			//				return
			//			}
			//			snapshot = result
			//			continue
			//		}
			//	}
			//} else {
			//
			//	if orderbookBTCData.Data.FinalUpdateLastStream != snapshot.LastUpdateID {
			//		log.Printf("gap detected: pu=%d snapshot=%d", orderbookBTCData.Data.FinalUpdateLastStream, snapshot.LastUpdateID)
			//		log.Printf("gap detected, restarting")
			//		break
			//	}
			//}

			//apply bid updates
			for _, upd := range orderbookBTCData.Data.Bids {
				price := upd[0]
				qty := upd[1]

				qp, err := strconv.ParseFloat(qty, 64)
				if err != nil {
					log.Printf("Failed to parse qty: %v", err)
					continue
				}

				if qp == 0 {
					// remove price from snapshot.Bids if exists
					delete(snapshot.Bids, price)
				} else {
					// update qty if exists, else append
					snapshot.Bids[price] = qty
				}
			}

			//apply ask updates
			for _, upd := range orderbookBTCData.Data.Asks {
				price := upd[0]
				qty := upd[1]

				qp, err := strconv.ParseFloat(qty, 64)
				if err != nil {
					log.Printf("Failed to parse qty: %v", err)
					continue
				}

				if qp == 0 {
					// remove price from snapshot.Asks if exists
					delete(snapshot.Asks, price)
				} else {
					// update qty if exists, else append
					snapshot.Asks[price] = qty
				}
			}
			snapshot.LastUpdateID = orderbookBTCData.Data.FinalUpdate

			ch <- SortedOrderBookSnapshot{
				LastUpdateID: snapshot.LastUpdateID,
				Bids:         sortBids(snapshot.Bids),
				Asks:         sortAsks(snapshot.Asks),
			}
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

	var rawSnapshot rawSnapshot
	var snapshot OrderBookSnapshot
	if err := json.Unmarshal(body, &rawSnapshot); err != nil {
		log.Printf("Failed to unmarshal snapshot: %v", err)
		return nil, err
	}

	snapshot.LastUpdateID = rawSnapshot.LastUpdateID
	snapshot.Bids = make(map[string]string)
	snapshot.Asks = make(map[string]string)

	for _, upd := range rawSnapshot.Bids {
		price := upd[0]
		qty := upd[1]

		qp, err := strconv.ParseFloat(qty, 64)
		if err != nil {
			log.Printf("Failed to parse qty: %v", err)
			continue
		}

		if qp == 0 {
			// remove price from snapshot.Bids if exists
			delete(snapshot.Bids, price)
		} else {
			// update qty if exists, else append
			snapshot.Bids[price] = qty
		}
	}

	for _, upd := range rawSnapshot.Asks {
		price := upd[0]
		qty := upd[1]

		qp, err := strconv.ParseFloat(qty, 64)
		if err != nil {
			log.Printf("Failed to parse qty: %v", err)
			continue
		}

		if qp == 0 {
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
func sortBids(bids map[string]string) [][]string {
	keys := make([]string, 0, len(bids))
	for k := range bids {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		pi, _ := strconv.ParseFloat(keys[i], 64)
		pj, _ := strconv.ParseFloat(keys[j], 64)
		return pi > pj
	})
	result := make([][]string, 0, len(keys))
	for _, k := range keys {
		result = append(result, []string{k, bids[k]})
	}
	return result
}

func sortAsks(asks map[string]string) [][]string {
	keys := make([]string, 0, len(asks))
	for k := range asks {
		keys = append(keys, k)
	}

	sort.Slice(keys, func(i, j int) bool {
		pi, _ := strconv.ParseFloat(keys[i], 64)
		pj, _ := strconv.ParseFloat(keys[j], 64)
		return pi < pj
	})
	result := make([][]string, 0, len(keys))
	for _, k := range keys {
		result = append(result, []string{k, asks[k]})
	}
	return result
}
