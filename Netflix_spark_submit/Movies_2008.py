# spark-submit --driver-class-path /usr/lib/jvm/java-17-openjdk-amd64/lib/postgresql-42.5.0.jar Netflix_spark_submit/Movies_2008.py


from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName('')\
    .getOrCreate()

Netflix_Titles = spark.read.json('Data/netflix_titles.json')

Netflix_Titles.show(20)


# 5. How many movies were released in 2008?
#############################################

movies_2008 = Netflix_Titles.select(Netflix_Titles['type'], Netflix_Titles['title'], Netflix_Titles['release_year']).filter(
    (Netflix_Titles['release_year'] == '2008') & (Netflix_Titles['type'] == 'Movie'))

movies_2008.show(20)

print('count:  Movies released in 2008:  ', movies_2008.count())

############ CSV ############
movies_2008.coalesce(1).write.csv(
    'Output/Movies_2008', header=True, mode='overwrite')

# # ############ Postgres ############
movies_2008.write.format('jdbc').options(url='jdbc:postgresql://127.0.0.1/postgres', driver='org.postgresql.Driver',
                                         dbtable='Movies_2008', user='amrit', password='1234').mode('overwrite').save()
