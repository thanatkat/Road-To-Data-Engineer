

## Install PyMySQL 
ซึ่งเป็น package สำหรับเชื่อมต่อ MySQL database


! pip install pymysql

# TODO: ใส่ database credential
import os

"""อ่านเชื่อไฟล์ด้วย bash command `cat ชื่อไฟล์` """

!cat .env

"""## การอ่านตัวแปร .env จากไฟล์ง่าย ๆ ด้วย python-dotenv
เริ่มจาก install แพ็คเกจ python-dotenv ก่อน 

"""

!pip install python-dotenv

"""เรียกใช้งานและอ่านตัวแปรจาก .env เข้ามา"""

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

"""cursors เป็น object ของ database ที่เอาไว้ใช้ในการเข้าถึง data การใช้งานจึงเป็นไปตามการออกแบบของแต่ละ database  ในกรณีนี้ก็จะใช้อ้างอิงตาม documentation ของ PyMySQL แล้วสามารถอ้างอิง                                                                                                                                                                                                                                                                                            โค้ดตามนั้นได้เลย https://pymysql.readthedocs.io/en/latest/user/examples.html """

connection

"""ตัวแปร connection นี้ เราได้ connect ต่อเข้ากับ database เอาไว้แล้ว
## List Tables
เรามาลองดูกันว่ามี table อะไรในนั้นบ้าง
"""

# list all tables ด้วย SQL คำสั่ง show tables;
cursor = connection.cursor()
cursor.execute("show tables;")
tables = cursor.fetchall()
cursor.close()
print(tables)

"""`show tables` เป็น SQL ในการลิสต์ table ออกมา

## Query Table

การใช้ `with connection.cursor() as cursor:` จะจัดการ scope ของการเรียกใช้งาน cursor ให้  ในที่นี้ถือว่าได้สร้างตัวแปร cursor แล้วในคำสั่ง with และ ไม่ต้องใช้ cursor.close()
"""

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

"""

เราก็ได้ data table แรกของเรามาแล้ว

**ข้อสังเกต**
ตัวเลขข้างหน้าสุดของ pandas ที่เป็น 0 ถึง (จำนวน rows - 1) ในที่นี้คือ 0 - 2268 เรียกว่า **index** 

index คือ สิ่งที่ pandas เอาไว้ใช้เก็บ key ในแต่ละ row เอาไว้ โดยถ้าไม่กำหนด index มาก็จะสร้างให้เหมือนในตัวอย่าง

แต่ในที่นี้เรามี Book_ID ที่เป็นตัวเลย unique ประจำแถวอยู่แล้ว สามารถกำหนด index เป็น Book_ID ได้ เพื่อลดความซ้ำซ้อน
"""

audible_data.set_index("Book_ID")

"""เท่านี้ก็สามารถ เอา Book_ID มาเป็น index ได้แล้ว

ถ้าไม่อยาก set_index() ทีหลังก็ สามารถใส่ `index_col="Book_ID"` เพิ่มเข้าไปในบรรทัดที่สร้าง DataFrame เลยได้ 
```
audible_data = pd.DataFrame(result), index_col="Book_ID"
```

## อีกวิธีหนึ่งในการ query โดยใช้ Pandas สะดวกมาก ๆ

แต่ว่า เนื่องจากว่า table เรามีสอง table เรามาดูอีกวิธีหนึ่งที่สะดวกขึ้น โดยใช้ `read_sql()` ของ pandas
"""

sql = "SELECT * FROM audible_transaction"
audible_transaction = pd.read_sql(sql, connection)
audible_transaction

"""โค้ดสวยขึ้นมาก ๆ สองบรรทัดจบ ( ❛ ᴗ ❛ )
ถึงเวลาต้อง join ข้อมูลของสอง table

# Join table: audible_transaction & audible_data

ใน transaction dataframe เราจะไม่เห็นราคาและชื่อสินค้า ถ้าเราอยากรู้ว่าแต่ละ transaction มีจำนวนเงินเท่าไร จึงต้อง merge data รวมกับ dataframe ของ audible_data 

คีย์ที่ใช้ในการ merge คือ
- audible_transaction: `book_id`
- audible_data: `Book_ID`
"""

transaction = audible_transaction.merge(audible_data, how="left", left_on="book_id", right_on="Book_ID")

"""ผลลัพธ์จากการ join จะได้เป็นแบบนี้"""

transaction

"""ตอนนี้เราได้ข้อมูล transaction มาแล้ว แต่ว่าข้อมูล price เป็น USD (แถมยังเป็น string ที่มี $ ด้วย) 

ในส่วนถัดไป เราจะมาอ่าน data จาก API แปลงค่าเงิน เพื่อแปลงเป็นเงินบาท ตาม rate ของแต่ละวันในอดีตกัน ʕ•́ᴥ•̀ʔ

---


# Get data from REST API

หลังจากต่อกับ Database ได้แล้ว ก็อ่าน data จาก REST API กัน

Package `requests` ใช้สำหรับการเรียกใช้ REST API

(โดยปกติต้อง install package นี้เพิ่มเติม แต่ colab มี install ไว้อยู่แล้ว)

วิธีการ install: `pip install requests`
"""

import requests

"""ลองคลิกดูผลลัพธ์ผ่าน web browser ได้ [Currency conversion API](https://r2de2-workshop-vmftiryt6q-ts.a.run.app/usd_thb_conversion_rate)  การที่สามารถเปิดผ่าน web browser โดยตรงได้ มักจะเป็นการใช้งาน API แบบ **GET**

ผลลัพธ์ที่ return กลับมาจะเป็นประเภท JSON
จึงต้องใช้ package `json` (built-in) เพื่อโหลดข้อมูลเป็น dictionary หรือสามารถใช้ `.json()` ของ request เพื่อแปลงได้

การที่เราสามารถยิง request และ output ออกมาได้เลยโดยที่ไม่ต้องสร้าง payload เพิ่ม ดังตัวอย่างนี้ เรียกว่า HTTP GET (ในกรณีอื่น ๆ สามารถเพิ่ม arguement หรือ query string เข้าไปใน URL ได้)

## Requests library
สามารถศึกษาวิธีการสร้าง request และการใช้งาน package `requests` [ได้ที่นี่](https://requests.readthedocs.io/en/master/)
"""

url = "https://r2de2-workshop-vmftiryt6q-ts.a.run.app/usd_thb_conversion_rate"
# TODO: ลองศึกษาวิธีการใช้งาน package requests จากลิ้งค์(ตัวแปร url) แล้วลองเขียนโค้ดเพื่อ call URL นี้
# ต้องการผลลัพธ์ให้อยู่ในรูปแบบของ dictionary ที่ชื่อว่า result_conversion_rate

"""มาเช็คประเภทข้อมูล"""

print(type(result_conversion_rate))
assert isinstance(result_conversion_rate, dict)

""" ## Convert to Pandas
 แปลงกันอีกครั้งหนึ่ง ʕ•́ᴥ•̀ʔ
"""

conversion_rate = pd.DataFrame(result_conversion_rate)

conversion_rate

"""แปลงจาก index เป็น column date ธรรมดาเพื่อความสะดวกในการ join กับ table transaction"""

conversion_rate = conversion_rate.reset_index().rename(columns={"index": "date"})
conversion_rate[:3]

"""# Join the data

ในตอนนี้เราจะนำข้อมูลการซื้อขายและข้อมูล Rate การแปลงค่าเงิน เราจะรวมข้อมูลจากทั้งสอง Dataframe มารวมกัน

เราจะนำข้อมูลจากทั้งสองมารวมกันผ่าน column date ใน transaction และ date ใน conversion_rate 

แต่ถ้าสังเกตดี ๆ แล้วจะพบว่า timestamp ใน retail จะเก็บข้อมูลในรูปแบบ timestamp ส่วน date ใน conversion_rate จะเก็บข้อมูลในรูปแบบ date (ที่เป็น string) เท่านั้น
"""

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

"""แต่ตอนนี้ column Price เรายังเป็น string (มีเครื่องหมาย $ อยู่ ต้องเอาออก)
ในที่นี้จะใช้ function apply ของ DataFrame ภายใน apply จะเขียนในรูปแบบของ function หรือเป็น lambda function คือ function ที่สร้างขึ้นมา เพื่อประมวลผลในแต่ละแถว

สุดท้าย แปลงประเภทตัวแปลง เป็น float เพื่อรองรับ จำนวนที่มีทศนิยม
"""

final_df["Price"] = final_df.apply(lambda x: x["Price"].replace("$",""), axis=1)
final_df["Price"] = final_df["Price"].astype(float)

final_df

"""พอ join ข้อมูลได้แล้ว เราก็ มา คูณ currency conversion กัน (Price * convertsion_rate)"""

# TODO: เพิ่ม column 'THBPrice' ที่เกิดจาก column Price * conversion_rate
# Hint: ลองดู apply function ของ pandas

"""อีกวิธีหนึ่ง"""

def convert_rate(price, rate):
  return price * rate

"""สามารถ drop column ที่ไม่จำเป็นต้องใช้ได้ เช่น date ที่ซ้ำซ้อนกับ timestamp

axis = 1 หมายถึง drop column (ถ้า axis=0 จะใช้ drop row ได้)

"""

final_df = final_df.drop("date", axis=1)

"""## Save to CSV

เซฟ final_df เป็นไฟล์ csv
โดยปกติ pandas จะเซฟ index (0,1,2,3) ติดมาให้ด้วย ถ้าไม่ต้องการจะต้องใส่ `index=False`
"""

# TODO: save "to csv" file

!head otuput.csv

print("== End of Workshop 1 ʕ•́ᴥ•̀ʔっ♡ จบจริง ๆ แล้ว ==")
