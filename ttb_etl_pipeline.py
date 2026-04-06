import os
import yfinance as yf
import pandas as pd
import numpy as np
import urllib.parse
from datetime import datetime, timedelta
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

# --- Setup DB ---
DB_PASS_SAFE = urllib.parse.quote_plus(os.getenv('DB_PASS'))
DATABASE_URL = f"postgresql://{os.getenv('DB_USER')}:{DB_PASS_SAFE}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
engine = create_engine(DATABASE_URL)

def log_to_supabase(event, status, message):
    """ฟังก์ชันพิมพ์ Log ลงตาราง process_logs เหมือน Discord Logs"""
    with engine.begin() as conn:
        conn.execute(text("INSERT INTO process_logs (event_type, status, message) VALUES (:e, :s, :m)"),
                     {"e": event, "s": status, "m": message})

def get_last_date(table_name):
    """หาว่าข้อมูลล่าสุดในตารางคือวันที่เท่าไหร่"""
    try:
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT MAX(date) FROM {table_name}")).fetchone()
            return result[0]
    except:
        return None

# --- Indicator Functions (ฉบับสมบูรณ์) ---
def apply_indicators(df):
    """คำนวณ Indicators ทั้งหมดจากโค้ดที่ Sun ให้มา"""
    d = df.copy()
    # Trend
    d['ema_20'] = d['close'].ewm(span=20, adjust=False).mean()
    # MACD
    fast_ema = d['close'].ewm(span=12, adjust=False).mean()
    slow_ema = d['close'].ewm(span=26, adjust=False).mean()
    d['macd_line'] = fast_ema - slow_ema
    d['macd_signal'] = d['macd_line'].ewm(span=9, adjust=False).mean()
    d['macd_hist'] = d['macd_line'] - d['macd_signal']
    # Momentum
    delta = d['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
    d['rsi_14'] = 100 - (100 / (1 + (gain / loss)))
    # Stochastic
    low_min = d['low'].rolling(14).min()
    high_max = d['high'].rolling(14).max()
    d['stoch_k'] = 100 * ((d['close'] - low_min) / (high_max - low_min))
    d['stoch_d'] = d['stoch_k'].rolling(3).mean()
    # Volatility (Bollinger & ATR)
    mid = d['close'].rolling(20).mean()
    std = d['close'].rolling(20).std()
    d['bb_upper'] = mid + (std * 2)
    d['bb_lower'] = mid - (std * 2)
    tr = pd.concat([d['high']-d['low'], abs(d['high']-d['close'].shift()), abs(d['low']-d['close'].shift())], axis=1).max(axis=1)
    d['atr_14'] = tr.rolling(14).mean()
    # Volume
    tp = (d['high'] + d['low'] + d['close']) / 3
    mf = tp * d['volume']
    pos_mf = mf.where(tp > tp.shift(1), 0).rolling(14).sum()
    neg_mf = mf.where(tp < tp.shift(1), 0).rolling(14).sum()
    d['mfi_14'] = 100 - (100 / (1 + (pos_mf / neg_mf)))
    
    return d.dropna()

def run_pipeline():
    last_date = get_last_date("ttb_historical_data")
    ticker = yf.Ticker("TTB.BK")
    
    # 1. Fetch Incremental Data
    if last_date:
        fetch_from = (last_date + timedelta(days=1)).strftime('%Y-%m-%d')
        new_data = ticker.history(start=fetch_from)
        msg = f"Fetching since {fetch_from}"
    else:
        new_data = ticker.history(period="max")
        msg = "Initial full fetch"

    if new_data.empty:
        log_to_supabase("FETCH_RAW", "SKIPPED", "No new data found")
        return

    # บันทึก Raw Data
    new_data.index = new_data.index.date
    new_data.columns = [c.lower() for c in new_data.columns]
    new_data.index.name = 'date'
    new_data[['open', 'high', 'low', 'close', 'volume']].to_sql("ttb_historical_data", engine, if_exists='append', index=True)
    log_to_supabase("FETCH_RAW", "SUCCESS", f"Added {len(new_data)} rows. {msg}")

    # 2. Calculate Indicators (ดึงข้อมูลเก่ามาช่วยคำนวณเพื่อให้ค่าแม่นยำ)
    all_data = ticker.history(period="1y") # ดึง 1 ปีมาคำนวณเพื่อให้เส้น EMA/RSI นิ่ง
    all_data.columns = [c.lower() for c in all_data.columns]
    all_data.index = all_data.index.date
    all_data.index.name = 'date'
    
    indicators_df = apply_indicators(all_data)
    
    # บันทึกลง technical_indicators (ใช้ท่า Upsert)
    with engine.begin() as conn:
        indicators_df.to_sql("temp_indicators", conn, if_exists='replace')
        upsert_query = """
            INSERT INTO ttb_technical_indicators (date, ema_20, macd_line, macd_signal, macd_hist, rsi_14, stoch_k, stoch_d, bb_upper, bb_lower, atr_14, mfi_14)
            SELECT date, ema_20, macd_line, macd_signal, macd_hist, rsi_14, stoch_k, stoch_d, bb_upper, bb_lower, atr_14, mfi_14 FROM temp_indicators
            ON CONFLICT (date) DO UPDATE SET 
                ema_20=EXCLUDED.ema_20, rsi_14=EXCLUDED.rsi_14, macd_line=EXCLUDED.macd_line;
        """
        conn.execute(text(upsert_query))
    log_to_supabase("CALC_INDICATORS", "SUCCESS", "Indicators updated for latest period")

if __name__ == "__main__":
    run_pipeline()
