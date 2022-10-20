This repository is a collection of my practises and assignments for Apache spark during my trainee period in Fusemachines Nepal.

[Spark notes](https://amiright.notion.site/Apache-Spark-df9cc634edde48a497633d4f2a105936)

[Postgres in Ubuntu and WSL](https://amiright.notion.site/Postgres-in-WSL-7f9cb5767e5744489b77841cd248a60b)

## Create and activate a virtual environment:

`>> python -m venv env_name`

`>> env_name\Scripts\activate`

Use `pip install -r requirements.txt` to install the required packages.

## Information about the Files and Folders:
+ Data folder contains the  json files used for the assignment.
+ Output folder contains the output of questions in CSV format.
+ Netflix_spark_submit contains the python file for  netflix  question.
    + There is individual python file for each question.
    + And `All_in_one_Netflix.py` python file for all the questions combined
+ `Netflix.ipynb` is the jupyter notebook for the netflix question.
+ `Salaries.ipynb` is the jupyter notebook for the salaries question.

# ALERT!!  don't forget to trim the any `string` column before comparing (filtering) as whitespaces is present in both sides .

## Execution
There is two way to execute the code.

1. Using Jupyter notebook file `Netflix.ipynb` 
    
    + Configure driver path

            spark = SparkSession.builder.appName('Netflix')\
            .config('spark.driver.extraClassPath', '/usr/lib/jvm/java-17-openjdk-amd64/lib/postgresql-42.5.0.jar')\
            .getOrCreate()

    + Edit database connection string
 
            Dataframe_name.write.format('jdbc').options( url='jdbc:postgresql://127.0.0.1/Table_name', driver='org.postgresql.Driver', dbtable='Table_name', user='USER_NAME', password='1234').mode('overwrite').save()

2. Using `spark-submit` command 
    
    +       spark-submit --driver-class-path `path to jar` `path to python file` 
    + spark-submit code is provided at top of each python file.

### For Netflix

|**Question**|**spark-submit command**|
|---|---|
|All|*spark-submit --driver-class-path /usr/lib/jvm/java-17-openjdk-amd64/lib/postgresql-42.5.0.jar Netflix_spark_submit/All_in_one_Netflix.py*| 
|How many PG-13 titles are there?|*spark-submit --driver-class-path /usr/lib/jvm/java-17-openjdk-amd64/lib/postgresql-42.5.0.jar Netflix_spark_submit/PG_13.py*
|How many titles an actor or actress appeared in?|*spark-submit --driver-class-path /usr/lib/jvm/java-17-openjdk-amd64/lib/postgresql-42.5.0.jar Netflix_spark_submit/Cast_count.py*
|How many titles has a director has filmed?|*spark-submit --driver-class-path /usr/lib/jvm/java-17-openjdk-amd64/lib/postgresql-42.5.0.jar Netflix_spark_submit/Directors_count.py*
|What content is available in different countries?|*spark-submit --driver-class-path /usr/lib/jvm/java-17-openjdk-amd64/lib/postgresql-42.5.0.jar Netflix_spark_submit/Country_genre_combined.py*
|How many movies were released in 2008?|*spark-submit --driver-class-path /usr/lib/jvm/java-17-openjdk-amd64/lib/postgresql-42.5.0.jar Netflix_spark_submit/Movies_2008.py*
|List all the movies whose duration is greater than 100 mins ?|*spark-submit --driver-class-path /usr/lib/jvm/java-17-openjdk-amd64/lib/postgresql-42.5.0.jar Netflix_spark_submit/Movies_100.py*
|List movies played by “Kareena Kapoor” ?|*spark-submit --driver-class-path /usr/lib/jvm/java-17-openjdk-amd64/lib/postgresql-42.5.0.jar Netflix_spark_submit/Kareena_kapoor.py*





