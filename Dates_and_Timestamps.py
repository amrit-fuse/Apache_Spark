
# spark-submit Dates_and_Timestamps.py
# bash start-history-server.sh
# http://localhost:18080


from pyspark.sql.functions import format_number, dayofmonth, hour, dayofyear, month, year, weekofyear, date_format


from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Dates_and_Timestamps")\
    .config("spark.eventLog.enabled", "true")\
    .config("spark.eventLog.logDirectory", "tmp/spark-events")\
    .getOrCreate()


df = spark.read.csv("Data/appl_stock.csv", header=True, inferSchema=True)


df.show()

df.select(dayofmonth(df['Date'])).show()


df.select(hour(df['Date'])).show()


df.select(dayofyear(df['Date'])).show()  # count from 1st Jan


df.select(month(df['Date'])).show()

df.select(year(df['Date'])).show()


df.withColumn("Year", year(df['Date'])).select('Year').show()


newdf = df.withColumn("Year", year(df['Date']))
# select only 2 columns , year column is  renamed to avg(Year) and second colum selected is avg(Close)

newdf.groupBy("Year").mean()[['avg(Year)', 'avg(Close)']].show()


result = newdf.groupBy("Year").mean()[['avg(Year)', 'avg(Close)']]
result = result.withColumnRenamed("avg(Year)", "Year")
result = result.select('Year', format_number(
    'avg(Close)', 2).alias("Mean Close")).show()
