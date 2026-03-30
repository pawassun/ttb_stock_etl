📈 TTB Stock ETL Pipeline (Supabase + GitHub Actions)
โปรเจกต์สำหรับดึงข้อมูลหุ้น TMBThanachart Bank (TTB) อัตโนมัติ เพื่อนำไปจัดเก็บใน Cloud Database (Supabase) สำหรับใช้ในระบบ Line Chatbot และงาน Data Science
🛠️ Tech Stack
Language: Python 3.10+

Data Source: Yahoo Finance (yfinance)

Storage: PostgreSQL (Supabase)

Automation: GitHub Actions

Library หลัก: Pandas, SQLAlchemy, Psycopg2-binary, Dotenv

📂 โครงสร้างโปรเจกต์
ttb_etl_pipeline.py: โค้ดหลักสำหรับทำ ETL (Extract, Transform, Load) ทั้งแบบ Initial และ Daily Update

requirements.txt: รายการ Library ที่จำเป็นต้องใช้

.github/workflows/daily_etl.yml: (กำลังจะสร้าง) ไฟล์ตั้งค่าการรันอัตโนมัติบน GitHub

🚀 ระบบการทำงาน (Pipeline Logic)
Extract: ดึงข้อมูลหุ้น TTB.BK จาก Yahoo Finance API

Transform:

ทำ Data Cleaning เบื้องต้น

จัดการ Timezone ให้เป็น Asia/Bangkok

ตรวจสอบข้อมูลล่าสุดใน Database เพื่อป้องกันข้อมูลซ้ำ (Data Quality Check)

Load:

หากยังไม่มีตาราง จะทำ Initial Load ย้อนหลัง 5 ปี

หากมีตารางแล้ว จะทำ Append ข้อมูลใหม่รายวัน

⚙️ วิธีการใช้งาน (สำหรับ Local Development)
Clone Repository นี้ลงเครื่อง

สร้าง Virtual Environment และติดตั้ง Library:

Bash
pip install -r requirements.txt
สร้างไฟล์ .env และใส่ค่าการเชื่อมต่อ Supabase:

ข้อมูลโค้ด
DB_USER=your_user
DB_PASS=your_password
DB_HOST=your_host
DB_PORT=5432
DB_NAME=postgres
รัน Pipeline:

Bash
python ttb_etl_pipeline.py
📝 Logging & Monitoring
ระบบจะบันทึก Log การทำงานไว้ในไฟล์ ttb_pipeline.log เพื่อใช้ในการตรวจสอบ Error และความถูกต้องของข้อมูล (QA)