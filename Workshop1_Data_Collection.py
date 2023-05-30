## Install PyMySQL 

! pip install pymysql

# TODO: ใส่ database credential
import os

!cat .env

!pip install python-dotenv

import os
from dotenv import load_dotenv

load_dotenv()

class Config:
  MYSQL_HOST = os.getenv("MYSQL_HOST")
  MYSQL_PORT = int(os.getenv("MYSQL_PORT"))
  MYSQL_USER = os.getenv("MYSQL_USER")
  MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
  MYSQL_DB = os.getenv("MYSQL_DB")
  MYSQL_CHARSET = os.getenv("MYSQL_CHARSET")


# ทดลอง print จาก config
print(Config.MYSQL_PORT)

## Connect to DB
## หลังจากที่มี Credential ของ database แล้วก็สร้าง connection โดยการ connect ไปที่ DB ด้วย Config ของเรา

import pymysql

# Connect to the database
connection = pymysql.connect(host=Config.MYSQL_HOST,
                             port=Config.MYSQL_PORT,
                             user=Config.MYSQL_USER,
                             password=Config.MYSQL_PASSWORD,
                             db=Config.MYSQL_DB,
                             charset=Config.MYSQL_CHARSET,
                             cursorclass=pymysql.cursors.DictCursor)                                                                                                                                                                                                                                                                                    

# list all tables ด้วย SQL คำสั่ง show tables;
cursor = connection.cursor()
cursor.execute("show tables;")
tables = cursor.fetchall()
cursor.close()
print(tables)

## Query Table การใช้ `with connection.cursor() as cursor:` จะจัดการ scope ของการเรียกใช้งาน cursor ให้  ในที่นี้ถือว่าได้สร้างตัวแปร cursor แล้วในคำสั่ง with และ ไม่ต้องใช้ cursor.close()
# ใข้ with statement แทน cursor.close()
# TODO: มาลองเขียน SQL Query ข้อมูลจาก table audible_data ดูกัน

with connection.cursor() as cursor:
# query ข้อมูลจาก table audible_data

print("number of rows: ", len(result))

# สามารถดูผลลัพธ์ที่อ่าน result มาได้ 
result

# ดูประเภทของ result
type(result)

import pandas as pd

audible_data = pd.DataFrame(result)

type(audible_data)

audible_data

audible_data.set_index("Book_ID")

## อีกวิธีหนึ่งในการ query โดยใช้ Pandas สะดวกมาก ๆ

sql = "SELECT * FROM audible_transaction"
audible_transaction = pd.read_sql(sql, connection)
audible_transaction

# Join table: audible_transaction & audible_data

transaction = audible_transaction.merge(audible_data, how="left", left_on="book_id", right_on="Book_ID")

transaction

# อ่าน data จาก API แปลงค่าเงิน เพื่อแปลงเป็นเงินบาท ตาม rate ของแต่ละวันในอดีตกัน
# Get data from REST API

import requests
## Requests library

url = "https://r2de2-workshop-vmftiryt6q-ts.a.run.app/usd_thb_conversion_rate"
# ต้องการผลลัพธ์ให้อยู่ในรูปแบบของ dictionary ที่ชื่อว่า result_conversion_rate

print(type(result_conversion_rate))
assert isinstance(result_conversion_rate, dict)

## Convert to Pandas

conversion_rate = pd.DataFrame(result_conversion_rate)
conversion_rate

## แปลงจาก index เป็น column date ธรรมดาเพื่อความสะดวกในการ join กับ table transaction

conversion_rate = conversion_rate.reset_index().rename(columns={"index": "date"})
conversion_rate[:3]

transaction

# ก็อปปี้ column timestamp เก็บเอาไว้ใน column ใหม่ชื่อ date เพื่อที่จะแปลงวันที่เป็น date เพื่อที่จะสามารถนำมา join กับข้อมูลค่าเงินได้
transaction['date'] = transaction['timestamp']
transaction

# แปลงให้จาก timestamp เป็น date ในทั้ง 2 dataframe (transaction, conversion_rate)
transaction['date'] = pd.to_datetime(transaction['date']).dt.date
conversion_rate['date'] = pd.to_datetime(conversion_rate['date']).dt.date
transaction.head()

# TODO: รวม 2 dataframe (transaction, conversion_rate) เข้าด้วยกันด้วยคำสั่ง merge
# ผลลัพธ์สุดท้ายตั้งชื่อว่า final_df
# เอา $ ออก ในที่นี้จะใช้ function apply ของ DataFrame ภายใน apply จะเขียนในรูปแบบของ function หรือเป็น lambda function คือ function ที่สร้างขึ้นมา เพื่อประมวลผลในแต่ละแถว

final_df["Price"] = final_df.apply(lambda x: x["Price"].replace("$",""), axis=1)
final_df["Price"] = final_df["Price"].astype(float)

final_df

# พอ join ข้อมูลได้แล้ว เราก็ มา คูณ currency conversion กัน (Price * convertsion_rate)
# เพิ่ม column 'THBPrice' ที่เกิดจาก column Price * conversion_rate

final_df = final_df.drop("date", axis=1)

## Save to CSV เซฟ final_df เป็นไฟล์ csv โดยปกติ pandas จะเซฟ index (0,1,2,3) ติดมาให้ด้วย ถ้าไม่ต้องการจะต้องใส่ `index=False`
# TODO: save "to csv" file

!head otuput.csv
