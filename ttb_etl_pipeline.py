import os
import yfinance as yf
import pandas as pd
import numpy as np
import urllib.parse
import logging
from datetime import datetime, date, timedelta
from typing import Optional
from dotenv import load_dotenv
from sqlalchemy import create_engine, text, Engine

# --- Initial Setup ---
load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_engine() -> Engine:
    user = os.getenv('DB_USER')
    password = urllib.parse.quote_plus(os.getenv('DB_PASS', ''))
    host = os.getenv('DB_HOST')
    port = os.getenv('DB_PORT')
    dbname = os.getenv('DB_NAME')
    url = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
    return create_engine(url)

engine = get_engine()

def log_to_supabase(event: str, status: str, message: str) -> None:
    query = text("INSERT INTO process_logs (event_type, status, message) VALUES (:e, :s, :m)")
    try:
        with engine.begin() as conn:
            conn.execute(query, {"e": event, "s": status, "m": message})
    except Exception as e:
        logging.error(f"Failed to write log: {e}")

def get_last_date(table_name: str) -> Optional[date]:
    query = text(f"SELECT MAX(date) FROM {table_name}")
    try:
        with engine.connect() as conn:
            result = conn.execute(query).fetchone()
            return result[0] if result and result[0] else None
    except Exception:
        return None

# --- Indicator Engine (Multi-Window) ---
def apply_indicators(df: pd.DataFrame) -> pd.DataFrame:
    d: pd.DataFrame = df.copy()
    d.columns = [c.lower() for c in d.columns]
    windows = [10, 20, 30, 40, 50, 60]
    
    for w in windows:
        d[f'ema_{w}'] = d['close'].ewm(span=w, adjust=False).mean()
        
        delta = d['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=w).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=w).mean()
        d[f'rsi_{w}'] = 100 - (100 / (1 + (gain / (loss + 1e-9))))
        
        low_min = d['low'].rolling(window=w).min()
        high_max = d['high'].rolling(window=w).max()
        d[f'stoch_k_{w}'] = 100 * ((d['close'] - low_min) / (high_max - low_min + 1e-9))
        d[f'stoch_d_{w}'] = d[f'stoch_k_{w}'].rolling(window=3).mean()
        
        mid = d['close'].rolling(window=w).mean()
        std = d['close'].rolling(window=w).std()
        d[f'bb_upper_{w}'] = mid + (std * 2)
        d[f'bb_lower_{w}'] = mid - (std * 2)
        
        tr = pd.concat([d['high']-d['low'], abs(d['high']-d['close'].shift()), abs(d['low']-d['close'].shift())], axis=1).max(axis=1)
        d[f'atr_{w}'] = tr.rolling(window=w).mean()
        
        tp = (d['high'] + d['low'] + d['close']) / 3
        mf = tp * d['volume']
        pos_mf = mf.where(tp > tp.shift(1), 0).rolling(w).sum()
        neg_mf = mf.where(tp < tp.shift(1), 0).rolling(w).sum()
        d[f'mfi_{w}'] = 100 - (100 / (1 + (pos_mf / (neg_mf + 1e-9))))

    fast_ema = d['close'].ewm(span=12, adjust=False).mean()
    slow_ema = d['close'].ewm(span=26, adjust=False).mean()
    d['macd_line'] = fast_ema - slow_ema
    d['macd_signal'] = d['macd_line'].ewm(span=9, adjust=False).mean()
    d['macd_hist'] = d['macd_line'] - d['macd_signal']
    
    return d.dropna()

# --- Main Pipeline ---
def run_pipeline() -> None:
    symbol = "TTB.BK"
    raw_table = "ttb_historical_data"
    feature_table = "ttb_technical_indicators"
    windows = [10, 20, 30, 40, 50, 60]
    
    # --- 1. Raw Data Fetch (Incremental with Upsert) ---
    last_date_db = get_last_date(raw_table)
    ticker = yf.Ticker(symbol)
    
    # Check ป้องกันการดึงวันล่วงหน้า (Error: Start date after end date)
    if last_date_db and (last_date_db + timedelta(days=1)) > date.today():
        log_to_supabase("FETCH_RAW", "SKIPPED", f"Data already up to date ({last_date_db}).")
        new_raw = pd.DataFrame()
    else:
        start_date = (last_date_db + timedelta(days=1)) if last_date_db else "2021-01-01"
        new_raw = ticker.history(start=start_date)

    if not new_raw.empty:
        new_raw.index = new_raw.index.date
        new_raw.index.name = 'date'
        new_raw.columns = [c.lower() for c in new_raw.columns]
                
        try:
            with engine.begin() as conn:
                
                new_raw[['open', 'high', 'low', 'close', 'volume']].to_sql("temp_raw", conn, if_exists='replace', index=True)
                
                upsert_raw_query = f"""
                    INSERT INTO {raw_table} (date, open, high, low, close, volume)
                    SELECT date, open, high, low, close, volume FROM temp_raw
                    ON CONFLICT (date) DO UPDATE SET 
                        open = EXCLUDED.open,
                        high = EXCLUDED.high,
                        low = EXCLUDED.low,
                        close = EXCLUDED.close,
                        volume = EXCLUDED.volume;
                """
                conn.execute(text(upsert_raw_query))
                conn.execute(text("DROP TABLE IF EXISTS temp_raw;"))
            log_to_supabase("FETCH_RAW", "SUCCESS", f"Upserted {len(new_raw)} rows to {raw_table}.")
        except Exception as e:
            log_to_supabase("FETCH_RAW", "FAILED", str(e))
            logging.error(f"Raw data upsert failed: {e}")
    
    # --- 2. Indicators Update (Upsert) ---
    all_data = ticker.history(period="2y") 
    all_data.index = all_data.index.date
    all_data.index.name = 'date'
    indicators_df = apply_indicators(all_data)
    
    target_cols = ['macd_line', 'macd_signal', 'macd_hist']
    for w in windows:
        target_cols += [f'ema_{w}', f'rsi_{w}', f'stoch_k_{w}', f'stoch_d_{w}', 
                        f'bb_upper_{w}', f'bb_lower_{w}', f'atr_{w}', f'mfi_{w}']
    
    final_df = indicators_df[target_cols]

    try:
        with engine.begin() as conn:
            final_df.to_sql("temp_indicators", conn, if_exists='replace', index=True)
            cols = ", ".join([f'"{c}"' for c in final_df.columns])
            update_stmt = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in final_df.columns])
            upsert_query = f"""
                INSERT INTO {feature_table} ("date", {cols})
                SELECT "date", {cols} FROM temp_indicators
                ON CONFLICT ("date") DO UPDATE SET {update_stmt}, updated_at = CURRENT_TIMESTAMP;
            """
            conn.execute(text(upsert_query))
            conn.execute(text("DROP TABLE IF EXISTS temp_indicators;"))
        log_to_supabase("CALC_FEATURES", "SUCCESS", "Multi-window features updated.")
    except Exception as e:
        log_to_supabase("CALC_FEATURES", "FAILED", str(e))

if __name__ == "__main__":
    run_pipeline()
