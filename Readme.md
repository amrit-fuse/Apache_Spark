This repository is a collection of my practises and assignments for Apache spark during my trainee period in Fusemachines Nepal.

[Spark notes](https://amiright.notion.site/Apache-Spark-df9cc634edde48a497633d4f2a105936)

[Postgres in Ubuntu and WSL](https://amiright.notion.site/Postgres-in-WSL-7f9cb5767e5744489b77841cd248a60b)

## Create and activate a virtual environment:

`>> python -m venv env_name`

`>> env_name\Scripts\activate`

Use `pip install -r requirements.txt` to install the required packages.

## Information about the Files and Folders:
+ **Data** folder contains the  json files used for the assignment.
+ **Output** folder futher two folders
    + Netflix folder contains the output of the Netflix assignment in csv.
    + Salaries folder contains the output of the Salaries assignment in csv.
+ **Netflix_spark_submit** contains the python file for  netflix  question.
    + There is individual python file for each question.
    + And `All_in_one_Netflix.py` python file for all the questions combined
+ **Salaries_spark_submit** contains the python file for  salaries  question.
    +  `All_in_one_Salaries.py` python file for all the questions combined
+ `Netflix_titles.ipynb` is the jupyter notebook for the netflix question.
+ `Salaries.ipynb` is the jupyter notebook for the salaries question.

# ALERT!!  don't forget to trim the any `string` column before comparing (filtering) as whitespaces is present in both sides  in  *netflix_titles.json* 

# ALERT!!  don't forget to  lowercase and remove spaces  for JobTitle and EmployeeName column when comparing (filtering) in  *Salaries.json*

## Execution
There is two ways to execute the code.

1. Using Jupyter notebook file `Netflix_titles.ipynb`  and `Salaries.ipynb`
    
    + Configure driver path

            spark = SparkSession.builder.appName('Netflix')\
            .config('spark.driver.extraClassPath', '/usr/lib/jvm/java-17-openjdk-amd64/lib/postgresql-42.5.0.jar')\
            .getOrCreate()

    + Edit database connection string
 
            Dataframe_name.write.format('jdbc').options( url='jdbc:postgresql://127.0.0.1/Database_name', driver='org.postgresql.Driver', dbtable='Table_name', user='USER_NAME', password='1234').mode('overwrite').save()

2. Using `spark-submit` command 
    
    +       spark-submit --driver-class-path `path to jar` `path to python file` 
    + spark-submit code is provided at top of each python file.

### For Netflix_titles


|**Question**|**Remarks**|
|---|---|
|How many PG-13 titles are there?| filter `rating` column by `PG-13` and count the total rows|
|How many titles an actor or actress appeared in?|`cast` column has multiple values seperated by commas  so need to be exploded , trimed and empty rows neeed to be deleted and then group by `cast` column and count the total rows|
|How many titles has a director has filmed?|`director` column has multiple values seperated by commas  so need to be exploded , trimed and empty rows neeed to be deleted and then group by `director` column and count the total rows|
|What content is available in different countries?| `listed_in` column has multiple values seperated by commas  so need to be exploded , trimed and empty rows neeed to be deleted (listed_in and country) and then group by `country` column and concat the `listed_in` column|
|How many movies were released in 2008?|filter `release_year` column by `2008` and count the total rows|
|List all the movies whose duration is greater than 100 mins ?| first remove the `min` from the `duration` column and cast it to Integer and then filter the `duration` column by `100` and count the total rows|
|List movies played by “Kareena Kapoor” ?| explode the `cast` column  then trim it for spaces and filter the `cast` column by `Kareena Kapoor` |


### For Salaries
|**Question**|**Remarks**|
|---|---|
|Find the average salaries of each job position(assuming salary is the total of all pays).| group by `JobTitle` column and find the average of `TotalPay` column and remove null values|
|Which job title has the highest full time employees?|  filter the `JobTitle` column by `Full Time` and group by `JobTitle` column and count the total rows|
|List the name of employees who work for the police department?| filter the `JobTitle` column by containig *police*  and count the total rows|
|Find the job titles along with the employees name and ids.| group by `JobTitle` column and concat the `EmployeeName` and `Id` column| lowercase and remove spaces  for `JobTitle` and `EmployeeName` column  then groupby `JobTitle` column and concat the `Id` column|
|Find the number of employees in each job title. | group by `JobTitle` column and count |
|List out the names and positions of employees whose total pay is greater than 180000.| filter the `TotalPay` column by `180000` |
|List the names and ids of employees who have never done overtime.| filter the `OvertimePay` column by `0.00` |







