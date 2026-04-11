--> TTB End-to-End ETL & Feature Engineering Pipeline
Automated Financial Data Pipeline with Multi-Window Technical Indicators

โปรเจกต์ระบบดึงข้อมูลและวิเคราะห์ทางเทคนิคของหุ้น TMBThanachart Bank (TTB.BK) แบบอัตโนมัติ โดยเน้นความถูกต้องของข้อมูล (Incremental Load) และการทำ Feature Engineering เพื่อรองรับงาน Data Science และการสร้าง Trading Bot

--> Tech Stack
Language: Python 3.11+
Data Source: Yahoo Finance API (yfinance)
Storage: Supabase (PostgreSQL)
Automation: GitHub Actions (CI/CD)

--> Project Structure
ttb_etl_pipeline.py: โค้ดหลักที่ควบคุม Logic ทั้งหมด (Incremental Fetch + Feature Calculation)
requirements.txt: รายการ Library ที่จำเป็นต้องใช้
.github/workflows/main.yml: ไฟล์ตั้งค่าการรันอัตโนมัติ 3 ช่วงเวลาต่อวัน (ตลาดเปิด/เที่ยง/ปิด)

--> Pipeline Logic (Advanced Features)
*Smart Incremental Load: ตรวจสอบวันที่ล่าสุดใน Database ก่อนดึงข้อมูลใหม่เสมอ เพื่อป้องกันการดึงซ้ำและประหยัด Bandwidth
*Multi-Window Feature Engineering: คำนวณ Indicators ทั้งหมด 8 ประเภท ใน 6 ช่วงเวลา (10, 20, 30, 40, 50, 60 วัน) รวมกว่า 39 Features
- Trend: EMA
- Momentum: RSI, Stochastic, MACD
- Volatility: Bollinger Bands, ATR
- Volume: Money Flow Index (MFI)

*Data Quality Guard: ระบบป้องกัน Division by Zero และการตรวจสอบวันที่ล้ำหน้า (Forward-date check)
*Audit Logging: บันทึกประวัติการทำงานลงตาราง process_logs ใน Supabase เพื่อการ Monitoring ระยะยาว

--> การตั้งค่าและใช้งาน (Local Development)
1. Setup Environment: ติดตั้ง library ทั้งหมดที่ต้องใช้ผ่านไฟล์ requirements.txt (pip install -r requirements.txt
2. Configuration: กำหนดค่าต่างๆใหม่โดยการ สร้างไฟล์ .env และระบุค่าเชื่อมต่อ Supabase (DB_USER = your_user / DB_PASS = your_password / DB_HOST = your_host / DB_PORT = your_port / DB_NAME = postgres)
3. Run Pipeline: รันไฟล์ python ttb_etl_pipeline.py

--> SQL Schema
เพื่อให้ระบบทำงานได้สมบูรณ์ จำเป็นต้องสร้าง 3 ตารางหลักใน Supabase:
1. ttb_historical_data: สำหรับเก็บราคา Open, High, Low, Close, Volume
2. ttb_technical_indicators: สำหรับเก็บค่า Features ที่คำนวณได้
3. process_logs: สำหรับเก็บสถานะการทำงาน (SUCCESS/FAILED/SKIPPED)

--> Automation Schedule
ระบบรันอัตโนมัติผ่าน GitHub Actions ในวันทำการ (จันทร์-ศุกร์):
10:30 น. 
13:00 น. 
17:00 น. 

