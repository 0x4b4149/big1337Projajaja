import uvicorn
import asyncio
import sqlite3
import aiohttp
import json
import time
import os
import requests
from datetime import datetime, timedelta
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

# --- 設定與常數 ---
DB_NAME = "trades.db"
HYPERLIQUID_INFO_URL = "https://api.hyperliquid.xyz/info"
POLL_INTERVAL = 30 

# --- 資料庫操作 ---
def init_db():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('CREATE TABLE IF NOT EXISTS tracked_users (address TEXT PRIMARY KEY)')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS trades (
            tid INTEGER PRIMARY KEY, user_address TEXT, coin TEXT, side TEXT,
            px TEXT, sz TEXT, time INTEGER, hash TEXT, raw_data TEXT
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS transfers (
            hash TEXT PRIMARY KEY, user_address TEXT, type TEXT, amount TEXT,
            token TEXT, time INTEGER, raw_data TEXT
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
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    new_count = 0
    for item in updates:
        delta = item.get('delta', {})
        amount_usdc = delta.get('usdc', "0")
        if float(amount_usdc) == 0: continue
        trans_type = "deposit" if float(amount_usdc) > 0 else "withdraw"
        display_amount = amount_usdc.replace('-', '')
        tx_hash = item.get('hash') or f"no_hash_{item.get('time')}"
        try:
            cursor.execute('''
                INSERT INTO transfers (hash, user_address, type, amount, token, time, raw_data)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (tx_hash, user_address, trans_type, display_amount, "USDC", item.get('time'), json.dumps(item)))
            new_count += 1
        except sqlite3.IntegrityError:
            pass
    conn.commit()
    conn.close()
    if new_count > 0:
        print(f"[{user_address}] 新增 {new_count} 筆資金紀錄 (存/提)。")

# --- 查詢邏輯 ---
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
    result = []
    for row in rows:
        data = json.loads(row['raw_data'])
        data['action_type'] = row['type']
        data['amount_usdc'] = row['amount']
        result.append(data)
    return result

# --- 核心分析邏輯 ---
def analyze_whale_activity():
    WHALE_DEFINITION_DAYS = 30
    ANALYSIS_PERIOD_HOURS = 24
    WHALE_TRANSFER_THRESHOLD_USDC = 100000.0
    NET_VOLUME_BUY_THRESHOLD_USDC = 500000.0
    NET_VOLUME_SELL_THRESHOLD_USDC = -500000.0
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    current_time_ms = time.time() * 1000
    whale_start_time_ms = current_time_ms - (WHALE_DEFINITION_DAYS * 86400 * 1000)
    cursor.execute("SELECT DISTINCT user_address FROM transfers WHERE token = 'USDC' AND CAST(amount AS REAL) >= ? AND time >= ?", 
                   (WHALE_TRANSFER_THRESHOLD_USDC, whale_start_time_ms))
    whale_addresses = [row[0] for row in cursor.fetchall()]

    if not whale_addresses:
        conn.close()
        return {"suggestion": "HOLD", "reasoning": "在定義的時間範圍內未找到符合條件的巨鯨。", "analysis_time_utc": datetime.utcnow().isoformat(), "whale_definition_days": WHALE_DEFINITION_DAYS, "analysis_period_hours": ANALYSIS_PERIOD_HOURS, "whale_transfer_threshold_usdc": WHALE_TRANSFER_THRESHOLD_USDC, "identified_whales_count": 0, "net_volume_usdc": 0.0, "buy_volume_usdc": 0.0, "sell_volume_usdc": 0.0, "identified_whales": []}

    analysis_start_time_ms = current_time_ms - (ANALYSIS_PERIOD_HOURS * 3600 * 1000)
    placeholders = ','.join('?' for _ in whale_addresses)
    query = f"SELECT side, CAST(px AS REAL) as price, CAST(sz AS REAL) as size FROM trades WHERE user_address IN ({placeholders}) AND time >= ?"
    params = whale_addresses + [analysis_start_time_ms]
    cursor.execute(query, params)
    recent_trades = cursor.fetchall()
    conn.close()

    buy_volume = sum(price * size for side, price, size in recent_trades if side == 'B')
    sell_volume = sum(price * size for side, price, size in recent_trades if side == 'A')
    net_volume = buy_volume - sell_volume

    if net_volume > NET_VOLUME_BUY_THRESHOLD_USDC:
        suggestion, reasoning = "BUY", f"過去 {ANALYSIS_PERIOD_HOURS} 小時內，巨鯨表現出強烈的淨買入行為。"
    elif net_volume < NET_VOLUME_SELL_THRESHOLD_USDC:
        suggestion, reasoning = "SELL", f"過去 {ANALYSIS_PERIOD_HOURS} 小時內，巨鯨表現出強烈的淨賣出行為。"
    else:
        suggestion, reasoning = "HOLD", f"過去 {ANALYSIS_PERIOD_HOURS} 小時內，巨鯨的買賣行為相對平衡或不活躍。"

    return {"suggestion": suggestion, "reasoning": reasoning, "analysis_time_utc": datetime.utcnow().isoformat(), "whale_definition_days": WHALE_DEFINITION_DAYS, "analysis_period_hours": ANALYSIS_PERIOD_HOURS, "whale_transfer_threshold_usdc": WHALE_TRANSFER_THRESHOLD_USDC, "identified_whales_count": len(whale_addresses), "net_volume_usdc": round(net_volume, 2), "buy_volume_usdc": round(buy_volume, 2), "sell_volume_usdc": round(sell_volume, 2), "identified_whales": whale_addresses}

# --- 背景任務 ---
async def fetch_data(session: aiohttp.ClientSession, address: str):
    try:
        async with session.post(HYPERLIQUID_INFO_URL, json={"type": "userFills", "user": address}) as resp:
            if resp.status == 200: save_trades(address, await resp.json())
    except Exception as e: print(f"Fetch fills error {address}: {e}")
    try:
        async with session.post(HYPERLIQUID_INFO_URL, json={"type": "userNonFundingLedgerUpdates", "user": address}) as resp:
            if resp.status == 200: save_transfers(address, await resp.json())
    except Exception as e: print(f"Fetch ledger error {address}: {e}")

async def tracker_loop():
    print("--- 交易與資金追蹤器已啟動 ---")
    async with aiohttp.ClientSession() as session:
        while True:
            addresses = get_tracked_addresses()
            if addresses:
                await asyncio.gather(*(fetch_data(session, addr) for addr in addresses))
            await asyncio.sleep(POLL_INTERVAL)

# --- Telegram Bot ---
async def bot_start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("歡迎使用巨鯨分析 Bot！🐳\n\n使用 /analysis 指令來獲取最新的市場巨鯨活動分析報告。" )

async def bot_analysis_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("正在獲取最新的巨鯨活動分析，請稍候...")
    try:
        data = analyze_whale_activity() # 直接呼叫分析函式
        whales_list_str = "\n".join(f"- `{addr}`" for addr in data.get('identified_whales', [])) or "無"
        message = f"""
📈 **巨鯨活動分析報告** 📈

**分析建議：{data.get('suggestion', 'N/A')}**
**主要原因**：{data.get('reasoning', 'N/A')}

--- **數據摘要** ---
分析時間 (UTC)：`{data.get('analysis_time_utc', 'N/A')}`
分析時長：過去 {data.get('analysis_period_hours', 'N/A')} 小時
發現巨鯨數量：`{data.get('identified_whales_count', 'N/A')}`

--- **交易量** ---
總淨交易量：`${data.get('net_volume_usdc'):,.2f}`
總買入量：`${data.get('buy_volume_usdc'):,.2f}`
總賣出量：`${data.get('sell_volume_usdc'):,.2f}`

--- **已識別的巨鯨地址** ---
{whales_list_str}"""
        await update.message.reply_text(message, parse_mode='Markdown')
    except Exception as e:
        await update.message.reply_text(f"❌ **發生未知錯誤** ：\n`{str(e)}`")

# --- FastAPI 生命週期 ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    # 啟動資料庫和追蹤器
    init_db()
    loop = asyncio.get_event_loop()
    loop.create_task(tracker_loop())

    # 啟動 Telegram Bot
    token = "8599137925:AAGa5E2DsEEr1ZMwHECGjZZ6-Kr2TEgype8"
    if not token:
        print("警告：TELEGRAM_TOKEN 環境變數未設定，Telegram Bot 將不會啟動。" )
    else:
        application = Application.builder().token(token).build()
        application.add_handler(CommandHandler("start", bot_start_command))
        application.add_handler(CommandHandler("analysis", bot_analysis_command))
        await application.initialize()
        await application.start()
        await application.updater.start_polling()
        print("--- Telegram Bot 已整合啟動 ---")
        
    yield # FastAPI 伺服器運行

    # 關閉 Bot
    if 'application' in locals():
        await application.updater.stop()
        await application.stop()
        await application.shutdown()
        print("--- Telegram Bot 已關閉 ---")

# --- FastAPI 應用與 API 端點 ---
app = FastAPI(lifespan=lifespan)

class TrackRequest(BaseModel): address: str
class WhaleAnalysisResponse(BaseModel):
    suggestion: str; reasoning: str; analysis_time_utc: str; whale_definition_days: int
    analysis_period_hours: int; whale_transfer_threshold_usdc: float; identified_whales_count: int
    net_volume_usdc: float; buy_volume_usdc: float; sell_volume_usdc: float
    identified_whales: list[str]

@app.post("/track")
async def track_address(req: TrackRequest):
    if len(req.address) != 42: raise HTTPException(status_code=400, detail="無效地址")
    add_tracked_address(req.address)
    return {"message": "Success", "address": req.address}

@app.get("/trades/{address}")
async def get_trades(address: str):
    return {"address": address, "count": (count := len(trades := get_trades_from_db(address))), "trades": trades}

@app.get("/history/{address}")
async def get_full_history(address: str):
    return {"address": address, "summary": {"total_trades": len(trades := get_trades_from_db(address)), "total_transfers": len(transfers := get_transfers_from_db(address))}, "transfers": transfers, "trades": trades}

@app.get("/analysis/whale-activity", response_model=WhaleAnalysisResponse)
async def get_whale_analysis():
    try:
        return analyze_whale_activity()
    except Exception as e:
        print(f"Error during whale analysis: {e}")
        raise HTTPException(status_code=500, detail="進行巨鯨分析時發生內部錯誤")

# --- 主程式執行 ---
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)