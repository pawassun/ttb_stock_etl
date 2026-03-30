import os
import yfinance as yf
import pandas as pd
import logging
import urllib.parse
from datetime import datetime
import pytz
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# --- 1. Setup Logging ---
logging.basicConfig(
    filename='ttb_pipeline.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    encoding='utf-8'
)

# --- 2. Load Environment & Connection ---
load_dotenv()
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

# จัดการรหัสผ่านที่มีเครื่องหมายพิเศษ (@, #, /)
safe_password = urllib.parse.quote_plus(DB_PASS)
DATABASE_URL = f"postgresql://{DB_USER}:{safe_password}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL)


def get_th_time():
    return datetime.now(pytz.timezone('Asia/Bangkok'))


def run_etl():
    symbol = "TTB.BK"
    table_name = "ttb_historical"
    now_str = get_th_time().strftime('%Y-%m-%d %H:%M')

    print(f"🚀 เริ่มต้นทำงาน Pipeline สำหรับ {symbol} ({now_str})")
    logging.info(f"--- Start Pipeline: {symbol} ---")

    try:
        # --- CHECK: ตรวจสอบว่ามีตารางอยู่แล้วหรือไม่ ---
        with engine.connect() as conn:
            check_table = conn.execute(text(
                f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{table_name}')"
            )).scalar()

        if not check_table:
            # --- MODE: INITIAL LOAD (5 ปี) ---
            print("📦 ไม่พบตารางเดิม กำลังทำ Initial Load (ย้อนหลัง 5 ปี)...")
            df = yf.Ticker(symbol).history(period="5y")
            df = df.reset_index()
            df['Date'] = pd.to_datetime(df['Date']).dt.date
            df = df[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']]

            df.to_sql(table_name, engine, if_exists='replace', index=False)
            msg = f"✅ Initial Load สำเร็จ: {len(df)} แถว"
            print(msg)
            logging.info(msg)

        else:
            # --- MODE: DAILY UPDATE ---
            print("🔄 ตารางพร้อมใช้งาน กำลังตรวจสอบข้อมูลใหม่...")
            with engine.connect() as conn:
                last_date = conn.execute(text(f"SELECT MAX(\"Date\") FROM {table_name}")).scalar()

            df_new = yf.Ticker(symbol).history(period="5d")  # ดึงเผื่อวันหยุด
            df_new = df_new.reset_index()
            df_new['Date'] = pd.to_datetime(df_new['Date']).dt.date
            df_new = df_new[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']]

            df_to_load = df_new[df_new['Date'] > last_date]

            if not df_to_load.empty:
                df_to_load.to_sql(table_name, engine, if_exists='append', index=False)
                msg = f"📥 อัปเดตข้อมูลใหม่สำเร็จ: {len(df_to_load)} แถว"
                print(msg)
                logging.info(msg)
            else:
                msg = "✅ ข้อมูลเป็นปัจจุบันอยู่แล้ว"
                print(msg)
                logging.info(msg)

    except Exception as e:
        err_msg = f"❌ Error: {str(e)}"
        print(err_msg)
        logging.error(err_msg, exc_info=True)


if __name__ == "__main__":
    run_etl()