
from pyspark.sql.functions import mean

# spark-submit Missing_Data.py
# bash start-history-server.sh
# http://localhost:18080

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Missing_Data")\
    .config("spark.eventLog.enabled", "true")\
    .config("spark.eventLog.logDirectory", "tmp/spark-events")\
    .getOrCreate()

df = spark.read.csv("Data/ContainsNull.csv", header=True, inferSchema=True)

df.show()

# Drop any row that contains missing data
df.na.drop().show()   # for how parameter default is 'any'

# Has to have at least 2 NON-null values
df.na.drop(thresh=2).show()


# Drop any row with null values in the Sales column
df.na.drop(subset=["Sales"]).show()


df.na.drop(how='any').show()  # Drop any row with any null values


df.na.drop(how='all').show()  # Drop any row with all null values

df.na.fill('NEW VALUE').show()


df.na.fill(0).show()

df.na.fill('No Name', subset=['Name']).show()


mean_val = df.select(mean(df['Sales'])).collect()

# Weird nested formatting of Row object!
mean_val[0][0]  # access the first element of the first row i.e mean value

mean_sales = mean_val[0][0]

df.na.fill(mean_sales, ["Sales"]).show()

# syntax     df.na.fill(value, subset=None)

# One (very ugly) one-liner
df.na.fill(df.select(mean(df['Sales'])).collect()[0][0], ['Sales']).show()
