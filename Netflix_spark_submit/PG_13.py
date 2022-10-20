# spark-submit --driver-class-path /usr/lib/jvm/java-17-openjdk-amd64/lib/postgresql-42.5.0.jar Netflix_spark_submit/PG_13.py


from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('PG_13')\
    .getOrCreate()


Netflix_Titles = spark.read.json('Data/netflix_titles.json')

Netflix_Titles.show(20)

####################################### 1. How many PG-13 titles are there? #########################################

# rating column  contains Rating for films and one of them is PG-13
PG_13 = Netflix_Titles.select('title', 'rating').filter(
    Netflix_Titles.rating == 'PG-13')

PG_13.show(20)

print('count of PG-13 rating', PG_13.count())

############ CSV ############
PG_13.coalesce(1).write.csv('Output/PG_13', header=True, mode='overwrite')

# ############ Postgres ############
PG_13.write.format('jdbc').options(url='jdbc:postgresql://127.0.0.1/postgres', driver='org.postgresql.Driver',
                                   dbtable='PG_13', user='amrit', password='1234').mode('overwrite').save()
# mode(overwrite) drop old table from database and creates a new table 'directors_count_no_empty'
# save() is action and it will execute the query
