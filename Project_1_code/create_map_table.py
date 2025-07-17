from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark import SparkConf
import os

conf = SparkConf().setAppName("ETL_by_June_July") \
        .set("spark.driver.memory", "4g") \
        .set("spark.executor.memory", "4g")
spark = SparkSession.builder.config(conf = conf).getOrCreate()

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
    print(f"Total raw records read: {df.count()}")  # In ra tổng số bản ghi thô sau khi union
    print("------------- Filtering data -------------")
    df = df.filter((col('user_id').isNotNull()) & (col('keyword').isNotNull()) & (col('action') == 'search'))
    print(f"Total records after filtering: {df.count()}")  # In ra tổng số bản ghi sau khi lọc
    return df

def most_searched(df, month):
    df = df.filter(col('month') == month)
    df = df.select("user_id", "keyword", 'month')
    df = df.groupBy("user_id", "keyword", 'month').count()
    df = df.withColumnRenamed('count','TotalSearch')
    # df = df.orderBy('TotalSearch', ascending=False)
    window = Window.partitionBy("user_id").orderBy(col("TotalSearch").desc())
    df = df.withColumn('Ranking', row_number().over(window))
    df = df.filter(col('Ranking')==1)
    df = df.withColumnRenamed('keyword', 'Most searched')
    df = df.select('user_id','Most searched', 'month')
    return df

def check_csv_output_file(folder_output, check_name):
    if not os.path.exists(folder_output):
        print(f"The folder {folder_output} doesn't exist")
        return False
    file = os.listdir(folder_output)
    csv_file = [f for f in file if f.endswith('.csv')]
    if csv_file:
        print(f"The csv file {csv_file} already exists")
        print("CSV ouput created successfully")
        return True
    else:
        print(f"The csv file {csv_file} doesn't exist")
        print("CSV ouput not created")
        return False

def main(folder_path):
    df = read_data_all(folder_path)
    print("------------- Processing most searched keywords per user by month -------------")
    # Process each month's data
    print("------------- June Data -------------")
    result_june = most_searched(df, '06')
    result_june.show(10, truncate=False)
    june_output_folder = "F:\\DE_study\\Big_data\\Buoi 7\\Most_Search\\June\\"
    print("------------- June csv file generating -------------")
    result_june.limit(1000).repartition(1).write.mode("overwrite").csv(june_output_folder,header=True)
    check_csv_output_file(june_output_folder, "June")

    print("------------- July Data -------------")
    result_july = most_searched(df, '07')
    result_july.show(10, truncate=False)
    july_output_folder = "F:\\DE_study\\Big_data\\Buoi 7\\Most_Search\\July\\"
    print("------------- July csv file generating -------------")
    result_july.limit(1000).repartition(1).write.mode("overwrite").csv(july_output_folder, header=True)
    check_csv_output_file(july_output_folder, "July")

    return result_june, result_july

folder_path = "F:\\Dataset\\MrLong Class\\log_search\\"
result_june, result_july = main(folder_path)
spark.stop()
