# spark-submit --driver-class-path /usr/lib/jvm/java-17-openjdk-amd64/lib/postgresql-42.5.0.jar Netflix_spark_submit/Directors_count.py

from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName('Directors_count')\
    .getOrCreate()

Netflix_Titles = spark.read.json('Data/netflix_titles.json')

Netflix_Titles.show(20)


######################### 3.  How many titles has a director has filmed? #########################################

# since multiple directors are separated by comma, we need to split them and then explode them into rows
Only_directors = Netflix_Titles.select(Netflix_Titles['director']).withColumn(
    'director', F.explode(F.split('director', ',')))

Only_directors = Only_directors.withColumn(
    'director', F.trim(Only_directors['director']))

directors_count = Only_directors.groupBy(
    'director').count().orderBy('count', ascending=False)

directors_count.show(20)

directors_count_no_empty = directors_count.filter(
    directors_count['director'] != '')  # empty values in director column are removed

directors_count_no_empty.show(20)  # empty values  not counted
directors_count_no_empty.count()


# # oneliner
# Netflix_Titles.select(Netflix_Titles['director']).withColumn('director', F.explode(F.split('director', ','))).groupBy('director').count().orderBy('count', ascending=False).show(5)  # count empty values too


############ CSV ############
directors_count_no_empty.coalesce(1).write.csv(
    'Output/Netflix/Directors_count', header=True, mode='overwrite')

# # ############ Postgres ############
directors_count_no_empty.write.format('jdbc').options(url='jdbc:postgresql://127.0.0.1/Netflix_titles',
                                                      driver='org.postgresql.Driver',  dbtable='Directors_count', user='amrit', password='1234').mode('overwrite').save()
