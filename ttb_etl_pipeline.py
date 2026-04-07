import os
import yfinance as yf
import pandas as pd
import numpy as np
import urllib.parse
import logging
from datetime import datetime, date, timedelta
from typing import Optional, Union, List, Tuple
from dotenv import load_dotenv
from sqlalchemy import create_engine, text, Engine

# --- 1. Initial Setup & Config ---
load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_engine() -> Engine:
    """สร้าง Connection Engine โดยจัดการรหัสผ่านที่มีตัวพิเศษ"""
    user = os.getenv('DB_USER')
    password = urllib.parse.quote_plus(os.getenv('DB_PASS', ''))
    host = os.getenv('DB_HOST')
    port = os.getenv('DB_PORT')
    dbname = os.getenv('DB_NAME')
    url = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
    return create_engine(url)

engine = get_engine()

# --- 2. Logging Function (Discord Style) ---
def log_to_supabase(event: str, status: str, message: str) -> None:
    """บันทึกประวัติการทำงานลงตาราง process_logs"""
    query = text("INSERT INTO process_logs (event_type, status, message) VALUES (:e, :s, :m)")
    try:
        with engine.begin() as conn:
            conn.execute(query, {"e": event, "s": status, "m": message})
    except Exception as e:
        logging.error(f"Failed to write log: {e}")

# --- 3. Incremental Helper ---
def get_last_date(table_name: str) -> Optional[date]:
    """หาว่าข้อมูลล่าสุดใน Database คือวันที่เท่าไหร่"""
    query = text(f"SELECT MAX(date) FROM {table_name}")
    try:
        with engine.connect() as conn:
            result = conn.execute(query).fetchone()
            return result[0] if result and result[0] else None
    except Exception as e:
        logging.warning(f"Table {table_name} check failed: {e}")
        return None

# --- 4. Technical Indicator Engine ---
def apply_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """คำนวณ Indicators ทั้งหมดจากโค้ดที่ Sun กำหนด"""
    d: pd.DataFrame = df.copy()
    d.columns = [c.lower() for c in d.columns]
    
    # [TREND] EMA 20
    d['ema_20'] = d['close'].ewm(span=20, adjust=False).mean()
    
    # [TREND] MACD
    fast_ema: pd.Series = d['close'].ewm(span=12, adjust=False).mean()
    slow_ema: pd.Series = d['close'].ewm(span=26, adjust=False).mean()
    d['macd_line'] = fast_ema - slow_ema
    d['macd_signal'] = d['macd_line'].ewm(span=9, adjust=False).mean()
    d['macd_hist'] = d['macd_line'] - d['macd_signal']
    
    # [MOMENTUM] RSI 14
    delta: pd.Series = d['close'].diff()
    gain: pd.Series = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss: pd.Series = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    d['rsi_14'] = 100 - (100 / (1 + (gain / loss)))
    
    # [MOMENTUM] Stochastic
    low_min: pd.Series = d['low'].rolling(window=14).min()
    high_max: pd.Series = d['high'].rolling(window=14).max()
    d['stoch_k'] = 100 * ((d['close'] - low_min) / (high_max - low_min))
    d['stoch_d'] = d['stoch_k'].rolling(window=3).mean()
    
    # [VOLATILITY] Bollinger Bands
    mid: pd.Series = d['close'].rolling(window=20).mean()
    std: pd.Series = d['close'].rolling(window=20).std()
    d['bb_upper'] = mid + (std * 2)
    d['bb_lower'] = mid - (std * 2)
    
    # [VOLATILITY] ATR 14
    tr: pd.Series = pd.concat([
        d['high'] - d['low'], 
        abs(d['high'] - d['close'].shift()), 
        abs(d['low'] - d['close'].shift())
    ], axis=1).max(axis=1)
    d['atr_14'] = tr.rolling(window=14).mean()
    
    # [VOLUME] MFI 14
    tp: pd.Series = (d['high'] + d['low'] + d['close']) / 3
    mf: pd.Series = tp * d['volume']
    pos_mf: pd.Series = mf.where(tp > tp.shift(1), 0).rolling(14).sum()
    neg_mf: pd.Series = mf.where(tp < tp.shift(1), 0).rolling(14).sum()
    d['mfi_14'] = 100 - (100 / (1 + (pos_mf / neg_mf)))
    
    return d.dropna()

# --- 5. Main Pipeline Operation ---
def run_pipeline() -> None:
    symbol: str = "TTB.BK"
    raw_table: str = "ttb_historical_data"
    feature_table: str = "ttb_technical_indicators"
    
    # --- Part 1: Raw Data (Incremental) ---
    last_date_in_db: Optional[date] = get_last_date(raw_table)
    ticker = yf.Ticker(symbol)
    
    if last_date_in_db:
        start_date: str = (last_date_in_db + timedelta(days=1)).strftime('%Y-%m-%d')
        new_raw: pd.DataFrame = ticker.history(start=start_date)
    else:
        new_raw: pd.DataFrame = ticker.history(period="max")

    if not new_raw.empty:
        new_raw.index = new_raw.index.date
        new_raw.index.name = 'date'
        new_raw.columns = [c.lower() for c in new_raw.columns]
        raw_to_db = new_raw[['open', 'high', 'low', 'close', 'volume']]
        raw_to_db.to_sql(raw_table, engine, if_exists='append', index=True)
        log_to_supabase("FETCH_RAW", "SUCCESS", f"Added {len(raw_to_db)} rows to {raw_table}.")
    else:
        log_to_supabase("FETCH_RAW", "SKIPPED", "No new raw data found.")

    # --- Part 2: Technical Indicators (Upsert) ---
    all_recent_data: pd.DataFrame = ticker.history(period="1y") # ใช้ 1 ปีเพื่อให้เลขแม่นยำ
    all_recent_data.index = all_recent_data.index.date
    all_recent_data.index.name = 'date'
    
    indicators_df: pd.DataFrame = apply_indicators(all_recent_data)
    
    # **สำคัญ: เลือกเฉพาะคอลัมน์ที่มีในตาราง Indicators เท่านั้น**
    target_columns = [
        'ema_20', 'macd_line', 'macd_signal', 'macd_hist', 
        'rsi_14', 'stoch_k', 'stoch_d', 'bb_upper', 
        'bb_lower', 'atr_14', 'mfi_14'
    ]
    final_features_df = indicators_df[target_columns]
    
    try:
        with engine.begin() as conn:
            final_features_df.to_sql("temp_indicators", conn, if_exists='replace', index=True)
            
            cols = ", ".join([f'"{c}"' for c in final_features_df.columns])
            update_stmt = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in final_features_df.columns])
            
            upsert_query = f"""
                INSERT INTO {feature_table} ("date", {cols})
                SELECT "date", {cols} FROM temp_indicators
                ON CONFLICT ("date") DO UPDATE SET 
                    {update_stmt},
                    updated_at = CURRENT_TIMESTAMP;
            """
            conn.execute(text(upsert_query))
            conn.execute(text("DROP TABLE IF EXISTS temp_indicators;"))
            
        log_to_supabase("CALC_FEATURES", "SUCCESS", f"Indicators updated in {feature_table}.")
    except Exception as e:
        log_to_supabase("CALC_FEATURES", "FAILED", f"Error: {str(e)}")

if __name__ == "__main__":
    run_pipeline()
