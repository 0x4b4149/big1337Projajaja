/*├── main.py              # FastAPI 主程式，資料抓取與 API
├── filter_address.py    # 交易分析與勝率計算
├── trades.db            # SQLite 資料庫（自動建立）
└── README.md

pip install fastapi uvicorn aiohttp pydantic

start
python main.py or uvicorn main:app --host 0.0.0.0 --port 8000

Post/track
{
  "address": "0x1234..."
}
Return  
{
  "message": "Success",
  "address": "0x1234..."
}
GET /trades/{address}
{
  "address": "...",
  "count": 10,
  "trades": [...]
}
GET /history/{address}
{
  "address": "...",
  "summary": {
    "total_trades": 10,
    "total_transfers": 3
  },
  "transfers": [...],
  "trades": [...]
}
TradeAnalyzer Function

get_all_tracked_addresses()

get_trades_by_address(address)

calculate_pnl_by_coin(trades)

calculate_winrate(address)

Formate of return back
{
  "overall_win_rate": 62.5,
  "total_trades": 16,
  "winning_trades": 10,
  "losing_trades": 6,
  "win_rates_by_coin": {
    "BTC": 70.0,
    "ETH": 50.0
  }
}

*/
