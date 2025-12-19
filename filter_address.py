import sqlite3
import json
from typing import Dict, List, Tuple
from collections import defaultdict

DB_NAME = "trades.db"

class TradeAnalyzer:
    def __init__(self, db_name: str = DB_NAME):
        self.db_name = db_name
    
    def get_all_tracked_addresses(self) -> List[str]:
        """獲取所有被追蹤的地址"""
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        cursor.execute("SELECT address FROM tracked_users")
        addresses = [row[0] for row in cursor.fetchall()]
        conn.close()
        return addresses
    
    def get_trades_by_address(self, address: str) -> List[dict]:
        """獲取指定地址的所有交易"""
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT raw_data FROM trades WHERE user_address = ? ORDER BY time ASC",
            (address,)
        )
        trades = [json.loads(row[0]) for row in cursor.fetchall()]
        conn.close()
        return trades
    
    def calculate_pnl_by_coin(self, trades: List[dict]) -> Dict[str, dict]:
        """
        計算每個幣種的盈虧
        返回格式: {
            'BTC': {
                'position': 0,  # 當前持倉
                'total_cost': 0,  # 總成本
                'realized_pnl': 0,  # 已實現盈虧
                'trades_count': 0
            }
        }
        """
        positions = defaultdict(lambda: {
            'position': 0.0,
            'total_cost': 0.0,
            'realized_pnl': 0.0,
            'trades_count': 0,
            'winning_trades': 0,
            'losing_trades': 0
        })
        
        for trade in trades:
            coin = trade.get('coin')
            side = trade.get('side')  # 'B' for buy, 'A' for sell
            px = float(trade.get('px', 0))
            sz = float(trade.get('sz', 0))
            
            if not coin or not side:
                continue
            
            pos = positions[coin]
            pos['trades_count'] += 1
            
            if side == 'B':  # 買入
                # 增加持倉和成本
                pos['total_cost'] += px * sz
                pos['position'] += sz
            
            elif side == 'A':  # 賣出
                if pos['position'] > 0:
                    # 計算平均成本
                    avg_cost = pos['total_cost'] / pos['position'] if pos['position'] > 0 else 0
                    # 計算這筆交易的盈虧
                    pnl = (px - avg_cost) * min(sz, pos['position'])
                    pos['realized_pnl'] += pnl
                    
                    # 記錄勝負
                    if pnl > 0:
                        pos['winning_trades'] += 1
                    elif pnl < 0:
                        pos['losing_trades'] += 1
                    
                    # 減少持倉和對應成本
                    cost_reduction = avg_cost * min(sz, pos['position'])
                    pos['total_cost'] -= cost_reduction
                    pos['position'] -= sz
                    
                    # 防止負數持倉
                    if pos['position'] < 0:
                        pos['position'] = 0
                        pos['total_cost'] = 0
        
        return dict(positions)
    
    def calculate_winrate(self, address: str) -> dict:
        """計算指定地址的勝率"""
        trades = self.get_trades_by_address(address)
        if not trades:
            return {"overall_win_rate": 0.0, "total_trades": 0, "winning_trades": 0, "losing_trades": 0, "win_rates_by_coin": {}}

        pnl_by_coin = self.calculate_pnl_by_coin(trades)

        total_winning_trades = 0
        total_losing_trades = 0
        total_trades = 0
        win_rates_by_coin = {}

        for coin, stats in pnl_by_coin.items():
            winning = stats['winning_trades']
            losing = stats['losing_trades']
            coin_total_trades = winning + losing
            
            total_winning_trades += winning
            total_losing_trades += losing
            total_trades += coin_total_trades

            if coin_total_trades > 0:
                win_rates_by_coin[coin] = (winning / coin_total_trades) * 100
            else:
                win_rates_by_coin[coin] = 0.0
        
        overall_win_rate = 0.0
        if total_trades > 0:
            overall_win_rate = (total_winning_trades / total_trades) * 100

        return {
            "overall_win_rate": overall_win_rate,
            "total_trades": total_trades,
            "winning_trades": total_winning_trades,
            "losing_trades": total_losing_trades,
            "win_rates_by_coin": win_rates_by_coin
        }