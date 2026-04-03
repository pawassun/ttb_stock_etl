import os
import yfinance as yf
import pandas as pd
import logging
import urllib.parse
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# --- 1. Setup Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# --- 2. Load Environment Variables ---
load_dotenv()
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')

# แก้ปัญหาเครื่องหมาย @ ใน Password
DB_PASS_SAFE = urllib.parse.quote_plus(DB_PASS)
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS_SAFE}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL)


def get_ttb_historical_data():
    """ดึงข้อมูลย้อนหลังตั้งแต่ปี 2021"""
    logging.info("📥 กำลังดึงข้อมูล TTB.BK ย้อนหลังตั้งแต่ 2021-01-01...")
    ticker = yf.Ticker("TTB.BK")
    # ดึงข้อมูลตั้งแต่วันที่ 1 ม.ค. 2021 จนถึงปัจจุบัน
    df = ticker.history(start="2021-01-01")
    df.index = df.index.date
    df.index.name = 'date'
    return df


def calculate_technical_indicators(df):
    """คำนวณ Features (MA, RSI, Daily Return)"""
    logging.info("⚙️ กำลังคำนวณ Technical Indicators...")
    feat_df = pd.DataFrame(index=df.index)

    # 1. Moving Averages (5 วัน และ 20 วัน)
    feat_df['ma_5'] = df['Close'].rolling(window=5).mean()
    feat_df['ma_20'] = df['Close'].rolling(window=20).mean()

    # 2. Daily Return (%) - กำไร/ขาดทุน รายวัน
    feat_df['daily_return'] = df['Close'].pct_change() * 100

    # 3. RSI (Relative Strength Index 14 days)
    delta = df['Close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    feat_df['rsi_14'] = 100 - (100 / (100 + rs))

    # ลบแถวที่มีค่าว่าง (20 วันแรกที่คำนวณไม่ได้) ออกเพื่อให้ข้อมูลสะอาด
    return feat_df.dropna()


def save_to_supabase(df, table_name):
    """บันทึกข้อมูลแบบ Upsert เข้าสู่ Supabase"""
    logging.info(f"📤 กำลังบันทึกข้อมูลลงตาราง {table_name} (จำนวน {len(df)} แถว)...")
    try:
        with engine.begin() as conn:
            # โหลดเข้า Temporary Table
            df.to_sql(f'temp_{table_name}', conn, if_exists='replace', index=True)

            # สร้างคำสั่ง Upsert (Update เมื่อวันที่ซ้ำ)
            cols = ", ".join([f'"{c}"' for c in df.columns])
            update_stmt = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in df.columns])

            upsert_query = f"""
                INSERT INTO {table_name} ("date", {cols})
                SELECT "date", {cols} FROM temp_{table_name}
                ON CONFLICT ("date") DO UPDATE SET {update_stmt};
            """
            conn.execute(text(upsert_query))
            conn.execute(text(f'DROP TABLE temp_{table_name}'))
        logging.info(f"✅ บันทึกตาราง {table_name} สำเร็จ!")
    except Exception as e:
        logging.error(f"❌ เกิดข้อผิดพลาดที่ {table_name}: {e}")


def run_backfill_pipeline():
    # 1. ดึงข้อมูลดิบย้อนหลัง (Raw Data)
    raw_df = get_ttb_historical_data()
    save_to_supabase(raw_df[['Open', 'High', 'Low', 'Close', 'Volume']], "ttb_raw")

    # 2. คำนวณและบันทึก Indicators (Features)
    features_df = calculate_technical_indicators(raw_df)
    save_to_supabase(features_df, "ttb_features")


if __name__ == "__main__":
    run_backfill_pipeline()
    logging.info("🏁 Pipeline Finished: Historical data from 2021 is now in Supabase!")
