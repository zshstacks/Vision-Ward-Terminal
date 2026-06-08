package binance

type BTCStruct struct {
	EventType             string      `json:"e"`
	EventTime             int64       `json:"E"`
	TransactionTime       int64       `json:"T"`
	Symbol                string      `json:"s"`
	FirstUpdate           int32       `json:"U"`
	FinalUpdate           int32       `json:"u"`
	FinalUpdateLastStream int32       `json:"pu"`
	Bids                  [][2]string `json:"b"` //[]slice [2]pair of strings
	Asks                  [][2]string `json:"a"`
}

func orderbookBTC() {}
