from june_july_ETL import result_june, result_july
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.getOrCreate()

# folder_path = "E:\\Dataset\\MrLong Class\\log_search"
# result_june, result_july = main(folder_path)

# Đọc mapping từ Excel
print("================= take mapping tables from sheet file  =================")
key_mapping_june = pd.read_excel("G:\\DE_Project\\Project_1\\Categorized_Keywords_Thang6_Thang7.xlsx", sheet_name='Thang 6')
key_mapping_july = pd.read_excel("G:\\DE_Project\\Project_1\\Categorized_Keywords_Thang6_Thang7.xlsx", sheet_name='Thang 7')

# Chuẩn hóa tên cột mapping để đảm bảo khớp với Spark DF
key_mapping_june.columns = [col.strip() for col in key_mapping_june.columns]
key_mapping_july.columns = [col.strip() for col in key_mapping_july.columns]

# Chuyển sang Spark DataFrame
df_mapping_june = spark.createDataFrame(key_mapping_june)
df_mapping_july = spark.createDataFrame(key_mapping_july)

# Chuẩn hóa tên cột để join
print("================= Standardizing columns  =================")
print("================= Ready to join  =================")
result_june = result_june.join(df_mapping_june, result_june['Most searched'] == df_mapping_june['keyword'], 'inner')\
    .select('user_id', 'Most searched', 'Category')\
    .withColumnRenamed('Most searched', 'Most_Search_T6')\
    .withColumnRenamed('Category', 'Category_T6')

result_july = result_july.join(df_mapping_july, result_july['Most searched'] == df_mapping_july['keyword'], 'inner')\
    .select('user_id', 'Most searched', 'Category')\
    .withColumnRenamed('Most searched', 'Most_Search_T7')\
    .withColumnRenamed('Category', 'Category_T7')

print("================= Joining data tables  =================")
df_all = result_june.join(result_july,'user_id', 'inner')
condition = col('Category_T6') == col('Category_T7')
df_all = df_all.withColumn('Category_change', when(condition, 'NoChange').otherwise(concat(df_all['Category_T6'], lit(' - '), df_all['Category_T7'])))

# Hiển thị kết quả
print("===== Kết quả Tháng 6 =====")
result_june.show(20, truncate=False)

print("===== Kết quả Tháng 7 =====")
result_july.show(20, truncate=False)

print("===== Counting df_all ==========")
print(df_all.count())
print("========== Last results ==========")
df_all.show(20, truncate=False)

