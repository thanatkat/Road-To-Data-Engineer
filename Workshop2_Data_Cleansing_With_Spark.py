# Step 1) ติดตั้ง Spark และ PySpark


!apt-get update                                                                          # อัพเดท Package ทั้งหมดใน VM ตัวนี้
!apt-get install openjdk-8-jdk-headless -qq > /dev/null                                  # ติดตั้ง Java Development Kit (จำเป็นสำหรับการติดตั้ง Spark)
!wget -q https://archive.apache.org/dist/spark/spark-3.1.2/spark-3.1.2-bin-hadoop2.7.tgz # ติดตั้ง Spark 3.1.2
!tar xzvf spark-3.1.2-bin-hadoop2.7.tgz                                                  # Unzip ไฟล์ Spark 3.1.2
!pip install -q findspark==1.3.0                                                         # ติดตั้ง Package Python สำหรับเชื่อมต่อกับ Spark

# Set enviroment variable ให้ Python รู้จัก Spark
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/content/spark-3.1.2-bin-hadoop2.7"

# ติดตั้ง PySpark ลงใน Python
!pip install pyspark==3.1.2

# สร้าง Spark Session เพิ้อใช้งาน Spark
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate()

# ดูเวอร์ชั่น Python
# Pyhton version 3.9.16 180423
import sys
sys.version_info

# ดูเวอร์ชั่น Spark
spark.version

# Download Data File
!wget -O data.zip https://file.designil.com/zdOfUE+
!unzip data.zip

# read csv file with Spark

dt = spark.read.csv('/content/ws2_data.csv', header = True, inferSchema = True, )

# Step 2) Data Profiling
# Data Profiling เป็นการทำความเข้าใจข้อมูลเบื้องต้น เพื่อที่เราจะได้รู้ว่าข้อมูลนี้มีคอลัมน์ไหนบ้าง ค่าโดยรวมเป็นอย่างไรบ้าง ฯลฯ เพื่อให้เราตัดสินใจได้ต่อว่าจะเช็คที่จุดไหนต่อไป

# ดูว่ามีคอลัมน์อะไรบ้าง
dt

# ดูข้อมูล
dt.show()

# ดูข้อมูล 100 แถวแรก
dt.show(100)

# ดูประเภทข้อมูลแต่ละคอลัมน์
dt.dtypes

# อีกคำสั่งในการดูข้อมูลแต่ละคอลัมน์ (Schema)
dt.printSchema()

# นับจำนวนแถวและ column
print((dt.count(), len(dt.columns)))

# สรุปข้อมูลสถิติ
# NaN = Not a Number
dt.describe().show()

# อีกคำสั่งในการสรุปข้อมูลสถิติ
# ReferenceL: https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.summary.html

dt.summary().show()

# สรุปข้อมูลสถิติเฉพาะ column ที่ระบุ
dt.select("price").describe().show()

# หา Missing Value บ้าง? และแสดงข้อมูลแถวที่มี Missing Value ให้ดูหน่อย

dt.summary("count").show()
dt.where(dt.user_id.isNull()).show()

# Step 3) EDA - Exploratory Data Analysis

## Non-Graphical EDA

# ข้อมูลที่เป็นตัวเลข
dt.where(dt.price >= 1).show()

# ข้อมูลที่เป็นตัวหนังสือ
dt.where(dt.country == 'Canada').show()

## Graphical EDA

import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd

# แปลง Spark Dataframe เป็น Pandas Dataframe - ใช้เวลาประมาณ 6 วินาที
dt_pd = dt.toPandas()

# ดูตัวอย่างข้อมูล
dt_pd.head()

# Boxplot - แสดงการกระจายตัวของข้อมูลตัวเลข
sns.boxplot(x = dt_pd['book_id'])

# Histogram - แสดงการกระจายตัวของข้อมูลตัวเลข
# bins = จำนวน bar ที่ต้องการแสดง
sns.histplot(dt_pd['price'], bins=10)

# book_id เพิ่มขึ้นตามราคาหรือเปล่า?

sns.scatterplot(x = dt_pd.book_id, y = dt_pd.price)
# book_id กับ price ไม่มีความสัมพันธ์กัน

# Other ans
sns.scatterplot(data=dt_pd, x = 'book_id', y = 'price')

# Plotly - interactive chart
import plotly.express as px
fig = px.scatter(dt_pd, 'book_id', 'price')
fig.show()

dt

# Show top 5 rows
dt.show(5)

# Show Schema
dt.printSchema()

dt.select("timestamp").show(10)

# แปลง string เป็น datetime
# create new column `withColumn`
from pyspark.sql import functions as f

dt_clean = dt.withColumn("timestamp",
                        f.to_timestamp(dt.timestamp, 'yyyy-MM-dd HH:mm:ss')
                        )
dt_clean.show()

dt_clean.printSchema()

"""## Anomalies Check

ใช้ Spark ตามหาสิ่งที่ผิดปกติในข้อมูล

### ความผิดปกติ 1) Syntactical Anomalies
**Lexical errors** เช่น สะกดผิด

#หาชื่อประเทศที่สะกดผิด แล้วแก้ชื่อที่สะกดผิดให้ถูก
"""

# ใน Data set ชุดนี้ มีข้อมูลจากกี่ประเทศ
dt_clean.select("Country").distinct().count()

# แทนที่ ... ด้วยจำนวนประเทศ เพื่อดูรายชื่อประเทศทั้งหมด
# sort = ทำให้ข้อมูลเรียงตามตัวอักษร อ่านง่ายขึ้น
# show() ถ้าไม่ใส่ตัวเลขจะขึ้นมาแค่ 20 อัน และใส่ False เพื่อให้แสดงข้อมูลในคอลัมน์แบบเต็ม ๆ (หากไม่ใส่ คอลัมน์ที่ยาวจะถูกตัดตัวหนังสือ)
dt_clean.select("Country").distinct().sort("Country").show(58, False )

# เปลี่ยน ... เป็นชื่อประเทศที่คุณคิดว่าผิด
dt_clean.where(dt_clean['Country'] == 'Japane').show()

# เปลี่ยน ... เป็นชื่อประเทศที่คุณคิดว่าผิด และ ...2 เป็นชื่อประเทศที่ถูกต้อง
from pyspark.sql.functions import when

dt_clean_country = dt_clean.withColumn("CountryUpdate", 
                                       when(dt_clean['Country'] == 'Japane', 'Japan').otherwise(dt_clean['Country'])
                                       )

# ตรวจสอบข้อมูลที่แก้ไขแล้ว
dt_clean_country.select("CountryUpdate").distinct().sort("CountryUpdate").show(58, False)

# ดูหน้าตาข้อมูลตอนนี้
dt_clean_country.show()

# เอาคอลัมน์ CountryUpdate ไปแทนที่คอลัมน์ Country
dt_clean = dt_clean_country.drop("Country").withColumnRenamed('CountryUpdate', 'Country')

# ดูหน้าตาข้อมูล
dt_clean.show()


### ความผิดปกติ 2) Semantic Anomalies

# ดูว่าข้อมูล user_id ตอนนี้หน้าตาเป็นอย่างไร
dt_clean.select("user_id").show(10)

# นับจำนวน user_id ทั้งหมด
dt_clean.select("user_id").count()

"""#### หาว่า user_id ตรงตามรูปแบบที่เราต้องการมั้ย และแทนที่ด้วยค่าที่ใกล้เคียงถ้าไม่ตรง

ดูว่า user_id ตรงตามรูปแบบที่เราต้องการ มีกี่แถว

"""

# แทนที่ ... ด้วย Regular Expression ของรูปแบบ user_id ที่เราต้องการ
dt_clean.where(dt_clean["user_id"].rlike("^[a-zA-Z0-9]{8}$")).count()

# แทนที่ ... ด้วย Regular Expression ของรูปแบบ user_id ที่เราต้องการ
dt_correct_userid = dt_clean.filter(dt_clean["user_id"].rlike("^[a-z0-9]{8}$"))
dt_incorrect_userid = dt_clean.subtract(dt_correct_userid)

dt_incorrect_userid.show(10)

"""มาทำการแก้ไข user_id นี้ให้ถูกต้องกันเถอะ (ตัวที่เป็น null ยังไม่ต้องแก้ไข)"""

# แทนค่าที่ผิด ด้วยค่าที่ถูกต้อง (โค้ดจาก Exercise 4)
dt_clean_userid = dt_clean.withColumn("User_id_update", 
                                       when(dt_clean['user_id'] == 'ca86d17200', 'ca86d172').otherwise(dt_clean['user_id'])
                                       )

# ตรวจสอบผลลัพธ์
dt_correct_userid = dt_clean_userid.filter(dt_clean_userid["user_id"].rlike("^[a-z0-9]{8}$"))
dt_incorrect_userid = dt_clean_userid.subtract(dt_correct_userid)

dt_incorrect_userid.show(10)

# เอาคอลัมน์ user_id_update ไปแทนที่ user_id (โค้ดจาก Exercise 4)
dt_clean = dt_clean_userid.drop("user_id").withColumnRenamed('User_id_update', 'user_id')

dt_clean.show(10)

"""#### ความผิดปกติ 3) Missing values

การเช็คและแก้ไข Missing Values (หากจำเป็น)

ค่า Missing Value คือ ค่าที่ว่างเปล่า

เราจะรู้ได้ยังไงว่าคอลัมน์ไหนมีค่าว่างเปล่ากี่ค่า
"""

# วิธีที่ 1 ในการเช็ค Missing Value
# ใช้เทคนิค List Comparehension - ทบทวนได้ใน Pre-course Python https://school.datath.com/courses/road-to-data-engineer-2/contents/6129b780564a8
# เช่น [ print(i) for i in [1,2,3] ]

# col = คำสั่ง Spark ในการเลือกคอลัมน์
# sum = คำสั่ง Spark ในการคิดผลรวม
from pyspark.sql.functions import col, sum

dt_nulllist = dt_clean.select([ sum(col(colname).isNull().cast("int")).alias(colname) for colname in dt_clean.columns ])
dt_nulllist.show()

# วิธีที่ 2 ในการเช็ค Missing Value - จาก Exercise 1 โค้ดสะอาดกว่ามาก แต่ต้องมาบวกลบเอง
dt_clean.summary("count").show()

# ดูช้อมูลว่าแถวไหนมี user_id เป็นค่าว่างเปล่า (โค้ดเดียวกับ Exercise 1)

dt_clean.where( dt_clean.user_id.isNull() ).show()

"""#### ทางทีม Data Analyst แจ้งว่าอยากให้เราแทน user_id ที่เป็น NULL ด้วย 00000000 ไปเลย
"""

# Answer here
dt_clean_user_id = dt_clean.withColumn("UserUpdate", 
                                       when(dt_clean['user_id'].isNull(), '00000000').otherwise(dt_clean['user_id'])
                                       )

dt_clean = dt_clean_user_id.drop("user_id").withColumnRenamed("UserUpdate", "user_id")

# เช็คว่า user ID ที่เป็น NULL หายไปแล้วจริงมั้ย
dt_clean.where( dt_clean.user_id.isNull() ).show()

"""### ความผิดปกติ 4) Outliers:

ข้อมูลที่สูงหรือต่ำผิดปกติจากข้อมูลส่วนใหญ่

มาลองใช้ Boxplot ในการหาค่า Outlier ของราคาหนังสือ
"""

# Cell นี้จะรันค่อนข้างนาน เนื่องจากข้อมูลมีเยอะ
dt_clean_pd = dt_clean.toPandas()

sns.boxplot(x = dt_clean_pd['price'])

"""เห็นได้ว่ามีหนังสือบางเล่มที่ราคาสูงกว่าปกติไปเยอะมาก ลองมาดูกันว่าหนังสือ book_id อะไรบ้าง ที่ราคาเกิน $80"""

dt_clean.where( dt_clean.price > 80 ).select("book_id").distinct().show()

"""เราสามารถนำ Book_ID อันนี้ไปเช็คต่อกับแหล่งข้อมูลได้ ว่าเป็นหนังสืออะไร และราคาเกิน $80 ผิดปกติมั้ย

ถ้าเอาไปเช็คในข้อมูลจาก Workshop 1 ก็จะพบว่า Book_ID = 635 คือ หนังสือชื่อ "The Power Broker"
https://www.audible.com/pd/The-Power-Broker-Audiobook/B0051JH67K?ipRedirectOverride=true&overrideBaseCountry=true&pf_rd_p=2756bc30-e1e4-4174-bb22-bce00b971761&pf_rd_r=MF7KC1JQF3A6GK2ET8XM

![](https://file.designil.com/7h1WIp+)

The Power Broker มีราคา $84 จริง และเป็นหนังสือเสียงที่มีความยาวถึง 66 ชั่วโมง

**ในที่นี้ ถือว่าเป็น Outlier จริง แต่ไม่ได้เป็นข้อมูลที่ผิด จึงไม่ต้องแก้อะไร**

### มาลอง Clean ข้อมูลด้วย Spark SQL

![alt text](https://cdn-std.droplr.net/files/acc_513973/881iHw)
"""

# แปลงข้อมูลจาก Spark DataFrame ให้เป็น TempView ก่อน
dt.createOrReplaceTempView("data")
dt_sql = spark.sql("SELECT * FROM data")
dt_sql.show()

# ลองแปลงโค้ดสำหรับลิสต์ชื่อประเทศ เป็น SQL
dt_sql_country = spark.sql("""
SELECT distinct country
FROM data
ORDER BY country
""")
dt_sql_country.show(100)

# ลองแปลงโค้ดสำหรับแทนที่ชื่อประเทศ Japane => Japan เป็น SQL
dt_sql_result = spark.sql("""
SELECT timestamp, user_id, book_id,
  CASE WHEN country = 'Japane' THEN 'Japan' ELSE country END AS country,
price
FROM data
""")
dt_sql_result.show()

# เช็คผลลัพธ์ว่าถูกจริงมั้ย
dt_sql_result.select("country").distinct().sort("country").show(58, False)

#เช็คว่ามีข้อมูล user_id ที่ไม่เป็นตัวหนังสือหรือตัวเลข 8 หลักมั้ย
dt_sql_result = spark.sql("""
SELECT *
FROM data
WHERE user_id NOT RLIKE '^[a-z0-9]{8}$'
""")
dt_sql_result.show()

# เช็คว่าข้อมูลที่ผิด หายไปหรือยัง
dt_sql_uid_result.where( dt_sql_uid_result.user_id == 'ca86d17200' ).show()

# เซฟเป็น partitioned files (ใช้ multiple workers)
dt_clean.write.csv('Cleaned_data.csv', header = True)

"""เราสามารถบังคับให้ Spark เซฟมาเป็นไฟล์เดียวได้"""

# เซฟเป็น 1 ไฟล์ (ใช้ single worker)
dt_clean.coalesce(1).write.csv('Cleaned_Data_Single.csv', header = True)
