# spark-submit --driver-class-path /usr/lib/jvm/java-17-openjdk-amd64/lib/postgresql-42.5.0.jar Netflix_spark_submit/Movies_100.py


from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName('')\
    .getOrCreate()

Netflix_Titles = spark.read.json('Data/netflix_titles.json')

Netflix_Titles.show(20)


############################################## 6. List all the movies whose duration is greater than 100 mins ? #############################################

#  remove min from duration column and convert(cast) to integer
Netflix_Titles = Netflix_Titles.withColumn('duration', F.regexp_replace(
    'duration', 'min', '')).withColumn('duration', F.col('duration').cast('int'))


##### Below code will result in error  because there  duration column  contain  min and season as well so.. as we  only replace min with empty string, season will remain as it is and during conversion  it will result in error #############

# def remove_min(duration):
#     return int(duration.replace('min', ''))
# remove_min_udf= F.udf(remove_min, IntegerType())

# Netflix_Titles=Netflix_Titles.withColumn('duration', remove_min_udf('duration'))

movies_100 = Netflix_Titles.select(Netflix_Titles['type'], Netflix_Titles['title'], Netflix_Titles['duration']).filter(
    (Netflix_Titles['duration'] > 100) & (Netflix_Titles['type'] == 'Movie')) .sort('duration', ascending=True)

movies_100.show(20)

print('count: movies with duration more than 100 :', movies_100.count())


############ CSV ############
movies_100.coalesce(1).write.csv(
    'Output/Movies_100', header=True, mode='overwrite')

# # ############ Postgres ############
movies_100.write.format('jdbc').options(url='jdbc:postgresql://127.0.0.1/postgres', driver='org.postgresql.Driver',
                                        dbtable='Movies_100', user='amrit', password='1234').mode('overwrite').save()
