import uvicorn
import asyncio
import sqlite3
import aiohttp
import json
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

# --- 設定與常數 ---
DB_NAME = "trades.db"
HYPERLIQUID_INFO_URL = "https://api.hyperliquid.xyz/info"
POLL_INTERVAL = 30 

# --- 資料庫操作 ---
def init_db():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    # 1. 追蹤名單
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tracked_users (
            address TEXT PRIMARY KEY
        )
    ''')
    
    # 2. 交易紀錄 (交換/買賣)
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

    # 3. 資金流水 (存入/提出) - 新增
    # 使用 hash + time 作為唯一鍵值，避免重複
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS transfers (
            hash TEXT PRIMARY KEY,
            user_address TEXT,
            type TEXT,     -- 'deposit' or 'withdraw'
            amount TEXT,   -- 金額
            token TEXT,    -- 通常是 USDC
            time INTEGER,
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
    cursor.execute("INSERT OR IGNORE INTO tracked_users (address) VALUES (?)", (address,))
    conn.commit()
    conn.close()

# --- 儲存邏輯 ---

def save_trades(user_address: str, trades: list):
    """儲存買賣/交換紀錄"""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    for t in trades:
        tid = t.get('tid')
        if not tid: continue
        try:
            cursor.execute('''
                INSERT INTO trades (tid, user_address, coin, side, px, sz, time, hash, raw_data)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (tid, user_address, t.get('coin'), t.get('side'), t.get('px'), t.get('sz'), t.get('time'), t.get('hash'), json.dumps(t)))
        except sqlite3.IntegrityError:
            pass
    conn.commit()
    conn.close()

def save_transfers(user_address: str, updates: list):
    """儲存存入與提出紀錄"""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    new_count = 0

    for item in updates:
        # Hyperliquid Ledger Update 結構通常包含: hash, time, delta(變動量)
        # 這裡簡化處理，主要針對 USDC 的變動
        delta = item.get('delta', {})
        amount_usdc = delta.get('usdc', "0")
        
        # 如果 USDC 變動是 0，可能是其他類型的內部轉帳，暫時忽略或設為 unknown
        if float(amount_usdc) == 0:
            continue

        is_deposit = float(amount_usdc) > 0
        trans_type = "deposit" if is_deposit else "withdraw"
        # 移除負號以便顯示
        display_amount = amount_usdc.replace('-', '')
        
        # 唯一識別碼 (hash)
        tx_hash = item.get('hash')
        if not tx_hash:
            # 如果沒有 hash，用時間戳記當作備用 ID
            tx_hash = f"no_hash_{item.get('time')}"

        try:
            cursor.execute('''
                INSERT INTO transfers (hash, user_address, type, amount, token, time, raw_data)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                tx_hash,
                user_address,
                trans_type,
                display_amount,
                "USDC",
                item.get('time'),
                json.dumps(item)
            ))
            new_count += 1
        except sqlite3.IntegrityError:
            pass # 已存在

    conn.commit()
    conn.close()
    if new_count > 0:
        print(f"[{user_address}] 新增 {new_count} 筆資金紀錄 (存/提)。")

# --- 查詢邏輯 (API) ---

def get_trades_from_db(user_address: str):
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("SELECT raw_data FROM trades WHERE user_address = ? ORDER BY time DESC", (user_address,))
    rows = cursor.fetchall()
    conn.close()
    return [json.loads(row['raw_data']) for row in rows]

def get_transfers_from_db(user_address: str):
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("SELECT raw_data, type, amount FROM transfers WHERE user_address = ? ORDER BY time DESC", (user_address,))
    rows = cursor.fetchall()
    conn.close()
    # 我們回傳稍微整理過的格式
    result = []
    for row in rows:
        data = json.loads(row['raw_data'])
        # 強制覆蓋一些易讀欄位
        data['action_type'] = row['type'] 
        data['amount_usdc'] = row['amount']
        result.append(data)
    return result

# --- 背景任務 ---

async def fetch_data(session: aiohttp.ClientSession, address: str):
    """同時抓取 '交易' 和 '資金流水'"""
    
    # 1. 抓取成交 (Trades/Swaps)
    payload_fills = {"type": "userFills", "user": address}
    try:
        async with session.post(HYPERLIQUID_INFO_URL, json=payload_fills) as resp:
            if resp.status == 200:
                data = await resp.json()
                if isinstance(data, list):
                    save_trades(address, data)
    except Exception as e:
        print(f"Fetch fills error {address}: {e}")

    # 2. 抓取資金流水 (Deposits/Withdrawals)
    # API 類型: userNonFundingLedgerUpdates
    payload_ledger = {"type": "userNonFundingLedgerUpdates", "user": address}
    try:
        async with session.post(HYPERLIQUID_INFO_URL, json=payload_ledger) as resp:
            if resp.status == 200:
                data = await resp.json()
                if isinstance(data, list):
                    save_transfers(address, data)
    except Exception as e:
        print(f"Fetch ledger error {address}: {e}")

async def tracker_loop():
    print("--- 交易與資金追蹤器已啟動 ---")
    async with aiohttp.ClientSession() as session:
        while True:
            addresses = get_tracked_addresses()
            if addresses:
                tasks = [fetch_data(session, addr) for addr in addresses]
                await asyncio.gather(*tasks)
            await asyncio.sleep(POLL_INTERVAL)

# --- FastAPI ---

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    loop = asyncio.get_event_loop()
    loop.create_task(tracker_loop())
    yield

app = FastAPI(lifespan=lifespan)

class TrackRequest(BaseModel):
    address: str

@app.post("/track")
async def track_address(req: TrackRequest):
    if len(req.address) != 42:
        raise HTTPException(status_code=400, detail="無效地址")
    add_tracked_address(req.address)
    return {"message": "Success", "address": req.address}

@app.get("/trades/{address}")
async def get_trades(address: str):
    """獲取指定地址的已儲存交易紀錄"""
    trades = get_trades_from_db(address)
    return {"address": address, "count": len(trades), "trades": trades}

@app.get("/history/{address}")
async def get_full_history(address: str):
    """
    取得該使用者的完整歷史：包含交換(Trades)與資金存提(Transfers)
    """
    trades = get_trades_from_db(address)
    transfers = get_transfers_from_db(address)
    
    return {
        "address": address,
        "summary": {
            "total_trades": len(trades),
            "total_transfers": len(transfers)
        },
        "transfers": transfers, # 存入與提出
        "trades": trades        # 交換與買賣
    }

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)