# spark-submit --driver-class-path /usr/lib/jvm/java-17-openjdk-amd64/lib/postgresql-42.5.0.jar Netflix_spark_submit/All_in_one_Netflix.py

from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName('')\
    .getOrCreate()

Netflix_Titles = spark.read.json('Data/netflix_titles.json')

Netflix_Titles.show(20)


############################# 1. How many PG-13 titles are there? #########################################

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


########################################## 2. How many titles an actor or actress appeared in? #########################################


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
    'Output/Cast_count', header=True, mode='overwrite')

# # ############ Postgres ############
cast_count_no_empty.write.format('jdbc').options(url='jdbc:postgresql://127.0.0.1/postgres',
                                                 driver='org.postgresql.Driver',  dbtable='Cast_count', user='amrit', password='1234').mode('overwrite').save()


########################################## 3.  How many titles has a director has filmed? #########################################

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
    'Output/Directors_count', header=True, mode='overwrite')

# # ############ Postgres ############
directors_count_no_empty.write.format('jdbc').options(url='jdbc:postgresql://127.0.0.1/postgres',
                                                      driver='org.postgresql.Driver',  dbtable='Directors_count', user='amrit', password='1234').mode('overwrite').save()


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
    'Output/Country_genre_combined', header=True, mode='overwrite')

# # ############ Postgres ############
country_genre_combined.write.format('jdbc').options(url='jdbc:postgresql://127.0.0.1/postgres', driver='org.postgresql.Driver',
                                                    dbtable='Country_genre_combined', user='amrit', password='1234').mode('overwrite').save()


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
