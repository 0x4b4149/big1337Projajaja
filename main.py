import asyncio
import sqlite3
import aiohttp
import json
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Body
from pydantic import BaseModel
from typing import List, Optional

# --- 設定與常數 ---
DB_NAME = "trades.db"
HYPERLIQUID_INFO_URL = "https://api.hyperliquid.xyz/info"
POLL_INTERVAL = 30  # 每 30 秒查詢一次 (避免觸發 Rate Limit)

# --- 資料庫操作 ---
def init_db():
    """初始化資料庫與資料表"""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    # 建立追蹤名單表
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tracked_users (
            address TEXT PRIMARY KEY
        )
    ''')
    
    # 建立交易紀錄表 (使用 tid 作為唯一鍵，防止重複)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS trades (
            tid INTEGER PRIMARY KEY,
            user_address TEXT,
            coin TEXT,
            side TEXT,
            px TEXT,
            sz TEXT,
            time INTEGER,
            hash TEXT,
            raw_data TEXT
        )
    ''')
    conn.commit()
    conn.close()

def get_tracked_addresses():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("SELECT address FROM tracked_users")
    rows = cursor.fetchall()
    conn.close()
    return [row[0] for row in rows]

def add_tracked_address(address: str):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    try:
        cursor.execute("INSERT OR IGNORE INTO tracked_users (address) VALUES (?)", (address,))
        conn.commit()
    finally:
        conn.close()

def save_trades(user_address: str, trades: list):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    new_count = 0
    
    for t in trades:
        # Hyperliquid 的 userFills 返回欄位包含: tid, coin, px, sz, side, time, hash 等
        # 我們使用 tid (Trade ID) 來判斷是否已經存過
        tid = t.get('tid')
        if not tid:
            continue
            
        try:
            cursor.execute('''
                INSERT INTO trades (tid, user_address, coin, side, px, sz, time, hash, raw_data)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                tid,
                user_address,
                t.get('coin'),
                t.get('side'),
                t.get('px'),
                t.get('sz'),
                t.get('time'),
                t.get('hash'),
                json.dumps(t)
            ))
            new_count += 1
        except sqlite3.IntegrityError:
            # tid 已存在，跳過
            pass
            
    conn.commit()
    conn.close()
    if new_count > 0:
        print(f"[{user_address}] 儲存了 {new_count} 筆新交易。")

def get_trades_from_db(user_address: str):
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row # 讓結果可以像字典一樣存取
    cursor = conn.cursor()
    cursor.execute("SELECT raw_data FROM trades WHERE user_address = ? ORDER BY time DESC", (user_address,))
    rows = cursor.fetchall()
    conn.close()
    # 將儲存的 JSON 字串轉回 Python 物件
    return [json.loads(row['raw_data']) for row in rows]

# --- 背景任務 ---
async def fetch_fills_for_user(session: aiohttp.ClientSession, address: str):
    """呼叫 Hyperliquid API 獲取最新成交"""
    payload = {
        "type": "userFills",
        "user": address
    }
    
    try:
        async with session.post(HYPERLIQUID_INFO_URL, json=payload) as response:
            if response.status == 200:
                data = await response.json()
                if isinstance(data, list):
                    save_trades(address, data)
                else:
                    print(f"[{address}] API 回傳格式錯誤: {data}")
            else:
                print(f"[{address}] 請求失敗 status: {response.status}")
    except Exception as e:
        print(f"[{address}] 發生錯誤: {e}")

async def tracker_loop():
    """持續運行的背景迴圈"""
    print("--- 交易追蹤器已啟動 ---")
    async with aiohttp.ClientSession() as session:
        while True:
            addresses = get_tracked_addresses()
            if addresses:
                tasks = [fetch_fills_for_user(session, addr) for addr in addresses]
                await asyncio.gather(*tasks)
            
            await asyncio.sleep(POLL_INTERVAL)

# --- FastAPI 應用 ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    # 啟動時執行
    init_db()
    loop = asyncio.get_event_loop()
    task = loop.create_task(tracker_loop())
    yield
    # 關閉時執行 (可選: 取消 task)
    
app = FastAPI(lifespan=lifespan)

# Pydantic 模型用於請求驗證
class TrackRequest(BaseModel):
    address: str

@app.post("/track")
async def track_address(req: TrackRequest):
    """新增一個要追蹤的地址"""
    if len(req.address) != 42 or not req.address.startswith("0x"):
        raise HTTPException(status_code=400, detail="無效的地址格式")
    
    add_tracked_address(req.address)
    return {"message": f"開始追蹤地址: {req.address}", "status": "success"}

@app.get("/trades/{address}")
async def get_trades(address: str):
    """獲取指定地址的已儲存交易紀錄"""
    trades = get_trades_from_db(address)
    return {"address": address, "count": len(trades), "trades": trades}

@app.get("/")
async def root():
    return {"message": "Hyperliquid Address Tracker is running. Use /track to add address."}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)