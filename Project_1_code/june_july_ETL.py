from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark import SparkConf
import os

spark = SparkSession.builder.getOrCreate()

def read_data_1_day(path, name):
    file_list = os.listdir(f'{path}/{name}')
    parquet_file = [file for file in file_list if file.endswith('.parquet')]
    df = spark.read.parquet(f'{path}/{name}/*.parquet')
    df = df.withColumn("month", lit(name[4:6]))
    return df

def read_data_all(folder_path):
    file_list = os.listdir(folder_path)
    print("------------- Read data from files -------------")
    df = read_data_1_day(folder_path, file_list[0])
    for file in file_list[1:]:
        df1 = read_data_1_day(folder_path, file)
        df = df.union(df1)
    df = df.cache()
    print("------------- Filtering data -------------")
    df = df.filter((col('user_id').isNotNull()) & (col('keyword').isNotNull()) & (col('action') == 'search'))
    return df

def most_searched(df, month):
    df = df.filter(col('month') == month)
    df = df.select("user_id", "keyword", 'month')
    df = df.groupBy("user_id", "keyword", 'month').count()
    df = df.withColumnRenamed('count','TotalSearch').orderBy('TotalSearch', ascending=False)
    # df = df.orderBy('TotalSearch', ascending=False)
    window = Window.partitionBy("user_id").orderBy(col("TotalSearch").desc())
    df = df.withColumn('Ranking', row_number().over(window))
    df = df.filter(col('Ranking')==1)
    df = df.withColumnRenamed('keyword', 'Most searched')
    df = df.select('user_id','Most searched', 'month')
    return df

def main(folder_path):
    df = read_data_all(folder_path)
    print("------------- Processing most searched keywords per user by month -------------")

    # Partition by month
    df_june = df.filter(col("month") == "06")
    df_july = df.filter(col("month") == "07")

    # Process each month's data
    print("------------- June Data -------------")
    result_june = most_searched(df_june, '06')
    result_june.show(10, truncate=False)

    print("------------- July Data -------------")
    result_july = most_searched(df_july,'07')
    result_july.show(10, truncate=False)

    return result_june, result_july

folder_path = "E:\\Dataset\\MrLong Class\\log_search"
result_june, result_july = main(folder_path)
