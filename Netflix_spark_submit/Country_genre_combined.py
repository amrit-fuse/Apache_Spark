# spark-submit --driver-class-path /usr/lib/jvm/java-17-openjdk-amd64/lib/postgresql-42.5.0.jar Netflix_spark_submit/Country_genre_combined.py


from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName('Country_genre')\
    .getOrCreate()

Netflix_Titles = spark.read.json('Data/netflix_titles.json')

Netflix_Titles.show(20)


########################################## 4. What content is available in different countries? #########################################

Only_genre_country = Netflix_Titles.select(Netflix_Titles['country'], Netflix_Titles['listed_in']).withColumn('country', F.explode(F.split(
    'country', ','))).withColumn('listed_in', F.explode(F.split('listed_in', ',')))  # here  both country aand listed_in needed to be exploded

Only_genre_country.show(20)


# trim both country and listed_in columns
Only_genre_country = Only_genre_country.withColumn(
    'country', F.trim(Only_genre_country['country']))
Only_genre_country = Only_genre_country.withColumn(
    'listed_in', F.trim(Only_genre_country['listed_in']))

# remove empty values in  listed_in  and country column
Only_genre_country = Only_genre_country.filter(
    Only_genre_country['listed_in'] != '')
Only_genre_country = Only_genre_country.filter(
    Only_genre_country['country'] != '')

country_genre_combined = Only_genre_country.groupBy('country').agg(F.concat_ws(',', F.collect_set('listed_in')).alias(
    'Genre')).sort('country')  # groupby country and then combine all the genres in a single column

country_genre_combined.show(20)

############ CSV ############
country_genre_combined.coalesce(1).write.csv(
    'Output/Netflix/Country_genre_combined', header=True, mode='overwrite')

# # ############ Postgres ############
country_genre_combined.write.format('jdbc').options(url='jdbc:postgresql://127.0.0.1/Netflix_titles', driver='org.postgresql.Driver',
                                                    dbtable='Country_genre_combined', user='amrit', password='1234').mode('overwrite').save()
