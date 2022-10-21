# spark-submit --driver-class-path /usr/lib/jvm/java-17-openjdk-amd64/lib/postgresql-42.5.0.jar Netflix_spark_submit/Cast_count.py

from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName('Cast_count')\
    .getOrCreate()

Netflix_Titles = spark.read.json('Data/netflix_titles.json')

Netflix_Titles.show(20)

# ######################################### 2. How many titles an actor or actress appeared in? #########################################


# new data frame after  split items in  cast column and then  expand/explode items in cast columns into rows
Only_cast = Netflix_Titles.select(Netflix_Titles['cast']).withColumn(
    'cast', F.explode(F.split('cast', ',')))

Only_cast = Only_cast.withColumn('cast', F.trim(Only_cast['cast']))

# groupby 'cast' and count the number of times each actor appeared in the data frame
cast_count = Only_cast.groupBy(
    'cast').count().orderBy('count', ascending=False)

cast_count.show(20)  # count empty values too

# empty values in cast column are removed
cast_count_no_empty = cast_count.filter(cast_count['cast'] != '')

cast_count_no_empty.show(20)  # empty values  not counted

# # oneliner
# Netflix_Titles.select(Netflix_Titles['cast']).withColumn('cast', F.explode(F.split('cast', ','))).groupBy('cast').count().orderBy('count', ascending=False).show(5)  # count empty values too


############ CSV ############
cast_count_no_empty.coalesce(1).write.csv(
    'Output/Netflix/Cast_count', header=True, mode='overwrite')

# # ############ Postgres ############
cast_count_no_empty.write.format('jdbc').options(url='jdbc:postgresql://127.0.0.1/Netflix_titles',
                                                 driver='org.postgresql.Driver',  dbtable='Cast_count', user='amrit', password='1234').mode('overwrite').save()
