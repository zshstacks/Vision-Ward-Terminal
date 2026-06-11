import { useEffect } from "react";
import { AppDispatch, RootState } from "./redux/store";
import { useDispatch, useSelector } from "react-redux";
import { updateOrderBook } from "./redux/cryptoSlice/cryptoSlice";
import "./App.css";

function App() {
  const dispatch: AppDispatch = useDispatch();
  const { lastUpdateId, asks, bids } = useSelector(
    (state: RootState) => state.crypto,
  );

  useEffect(() => {
    const socket = new WebSocket("ws://127.0.0.1:8080/ws");

    socket.onmessage = (event) => {
      const data = JSON.parse(event.data);
      dispatch(updateOrderBook(data));
    };

    return () => socket.close();
  }, []);
  return (
    <main className="bg-neutral-700 h-screen">
      <div className="flex justify-center items-center gap-2 m-auto h-screen">
        <div>
          <p className="text-white">Bids:</p>
          {bids.slice(0, 10).map((item, i) => (
            <div key={i} className="flex">
              <p className="bg-red-900 text-white mb-1">
                Price: {item[0]} / Quantity: {item[1]}
              </p>
            </div>
          ))}
        </div>
        <div>
          <p className="text-white">Asks:</p>
          {asks.slice(0, 10).map((item, i) => (
            <div key={i} className="flex">
              <p className="bg-green-700 text-white mb-1">
                Price: {item[0]} / Quantity: {item[1]}
              </p>
            </div>
          ))}
        </div>
      </div>
    </main>
  );
}

export default App;
