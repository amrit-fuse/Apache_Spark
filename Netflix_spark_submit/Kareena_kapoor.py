# spark-submit --driver-class-path /usr/lib/jvm/java-17-openjdk-amd64/lib/postgresql-42.5.0.jar Netflix_spark_submit/Kareena_kapoor.py


from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName('')\
    .getOrCreate()

Netflix_Titles = spark.read.json('Data/netflix_titles.json')

Netflix_Titles.show(20)


############################################## 7.  List movies played by “Kareena Kapoor” ? #############################################

title_cast = Netflix_Titles.select(Netflix_Titles['title'], Netflix_Titles['cast']).withColumn(
    'cast', F.explode(F.split('cast', ',')))

# remove empty values in cast column
title_cast = title_cast.filter(title_cast['cast'] != '')

# trim white spaces in cast column  as some values have white spaces and shows inaccuracy in results
title_cast = title_cast.withColumn('cast', F.trim(title_cast['cast']))

Kareena_kapoor = title_cast.filter(title_cast['cast'] == 'Kareena Kapoor')

Kareena_kapoor.show(20)

print(' Count :: Movies played by Kareena Kapoor are: ', Kareena_kapoor.count())

####### CSV file  ############
Kareena_kapoor.coalesce(1).write.csv(
    'Output/Kareena_kapoor', header=True, mode='overwrite')

############ Postgres ############
Kareena_kapoor.write.format('jdbc').options(url='jdbc:postgresql://127.0.0.1/postgres', driver='org.postgresql.Driver',
                                            dbtable='Kareena_kapoor', user='amrit', password='1234').mode('overwrite').save()
