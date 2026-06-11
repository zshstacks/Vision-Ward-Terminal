import { createSlice, PayloadAction } from "@reduxjs/toolkit";
import { SortedOrderBookSnapshot } from "../../types/reduxTypes";

const initialState: SortedOrderBookSnapshot = {
  lastUpdateId: 0,
  asks: [],
  bids: [],
};

const cryptoSlice = createSlice({
  name: "crypto",
  initialState,
  reducers: {
    updateOrderBook: (
      state,
      action: PayloadAction<SortedOrderBookSnapshot>,
    ) => {
      state.lastUpdateId = action.payload.lastUpdateId;
      state.bids = action.payload.bids;
      state.asks = action.payload.asks;
    },
  },
});

export const { updateOrderBook } = cryptoSlice.actions;
export default cryptoSlice.reducer;
