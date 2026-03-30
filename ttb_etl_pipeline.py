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

# จัดการรหัสผ่านที่มีเครื่องหมายพิเศษ (@, #, /) ด้วย URL Encoding
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
        # ตรวจสอบว่ามีตารางอยู่แล้วหรือไม่
        with engine.connect() as conn:
            check_table = conn.execute(text(
                f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{table_name}')"
            )).scalar()

        if not check_table:
            # --- MODE: INITIAL LOAD (5 ปี) ---
            print("📦 ไม่พบตารางเดิม กำลังทำ Initial Load...")
            df = yf.Ticker(symbol).history(period="5y")
            df = df.reset_index()

            # [QA Fix] ลบแถวที่ไม่มีราคา (Null) ออกก่อนบันทึก
            df = df.dropna(subset=['Close'])

            df['Date'] = pd.to_datetime(df['Date']).dt.date
            df = df[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']]

            df.to_sql(table_name, engine, if_exists='replace', index=False)
            logging.info(f"✅ Initial Load สำเร็จ: {len(df)} แถว")

        else:
            # --- MODE: DAILY UPDATE ---
            with engine.connect() as conn:
                last_date = conn.execute(text(f"SELECT MAX(\"Date\") FROM {table_name}")).scalar()

            df_new = yf.Ticker(symbol).history(period="5d")
            df_new = df_new.reset_index()

            # [QA Fix] ลบแถวที่เป็น NULL/NaN ออกก่อนตรวจสอบวันล่าสุด
            df_new = df_new.dropna(subset=['Close'])

            df_new['Date'] = pd.to_datetime(df_new['Date']).dt.date
            df_new = df_new[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']]

            # กรองเอาเฉพาะข้อมูลที่ใหม่กว่าวันล่าสุดที่มีใน DB จริงๆ
            df_to_load = df_new[df_new['Date'] > last_date]

            if not df_to_load.empty:
                df_to_load.to_sql(table_name, engine, if_exists='append', index=False)
                logging.info(f"📥 อัปเดตข้อมูลใหม่สำเร็จ: {len(df_to_load)} แถว")
            else:
                logging.info("✅ ข้อมูลเป็นปัจจุบันอยู่แล้ว")

    except Exception as e:
        logging.error(f"❌ Error: {str(e)}", exc_info=True)
        print(f"❌ เกิดข้อผิดพลาด ตรวจสอบรายละเอียดได้ที่ ttb_pipeline.log")


if __name__ == "__main__":
    run_etl()
