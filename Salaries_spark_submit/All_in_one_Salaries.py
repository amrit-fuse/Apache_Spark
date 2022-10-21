# spark-submit --driver-class-path /usr/lib/jvm/java-17-openjdk-amd64/lib/postgresql-42.5.0.jar Salaries_spark_submit/All_in_one_Salaries.py

from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName('Salaries_all_in_one')\
    .getOrCreate()

Salaries = spark.read.json('Data/Salaries.json')

Salaries.show()

Salaries.printSchema()


############################### # 1. Find the average salaries of each job position(assuming salary is the total of all pays).###################################


Avg_sal_job = Salaries.groupBy('jobTitle').agg(
    F.round(F.avg('Totalpay'), 2).alias('avg_salary')).sort('jobTitle')

Avg_sal_job = Avg_sal_job.filter(
    Avg_sal_job.avg_salary.isNotNull())  # removes null values

Avg_sal_job.show()

print('count: There are {} job titles'.format(Avg_sal_job.count()))

################ CSV ################
Avg_sal_job.coalesce(1).write.csv(
    'Output/Salaries/Avg_sal_job', header=True, mode='overwrite')


############### Postgres ###############
Avg_sal_job.write.format('jdbc').options(url='jdbc:postgresql://localhost:5432/Salaries',
                                         driver='org.postgresql.Driver', dbtable='Avg_sal_job', user='amrit', password='1234').mode('overwrite').save()


##############################################  # 2. Which job title has the highest full time employees?#################################################

Title_FT_Count = Salaries.filter(Salaries.Status == 'FT').groupBy('jobTitle').agg(
    F.count('jobTitle').alias('count')).sort('count', ascending=False)

Title_FT_Count.show()

print('Highest full time employee and count:', format(Title_FT_Count.first()))


############### CSV ################
Title_FT_Count.coalesce(1).write.csv(
    'Output/Salaries/Title_FT_Count', header=True, mode='overwrite')

############### Postgres ###############
Title_FT_Count.write.format('jdbc').options(url='jdbc:postgresql://localhost:5432/Salaries',
                                            driver='org.postgresql.Driver', dbtable='Title_FT_Count', user='amrit', password='1234').mode('overwrite').save()


# 3. List the name of employees who work for the police department?
#################################################
Emp_Police = Salaries.filter(Salaries.JobTitle.contains(
    'POLICE')).select('EmployeeName').sort('EmployeeName')

Emp_Police.show()

print('count: There are {} employees in police department'.format(Emp_Police.count()))

############### CSV ################
Emp_Police.coalesce(1).write.csv(
    'Output/Salaries/Emp_Police', header=True, mode='overwrite')

############### Postgres ###############
Emp_Police.write.format('jdbc').options(url='jdbc:postgresql://localhost:5432/Salaries',
                                        driver='org.postgresql.Driver', dbtable='Emp_Police', user='amrit', password='1234').mode('overwrite').save()


##############################################  # 4. Find the job titles along with the employees name and ids.                    #################################################
Title_Name_Id = Salaries.select(
    'JobTitle', 'EmployeeName', 'Id').sort('Employeename')

Title_Name_Id = Title_Name_Id.filter(Title_Name_Id.EmployeeName.isNotNull())

Title_Name_Id = Title_Name_Id.withColumn('EmployeeName', F.regexp_replace(
    'EmployeeName', '\s+', ' '))  # removes extra spaces in EmployeeName

Title_Name_Id_lower = Title_Name_Id.withColumn('JobTitle', F.lower(
    F.col('JobTitle'))).withColumn('EmployeeName', F.lower(F.col('EmployeeName')))

Title_Name_Ids = Title_Name_Id_lower.groupBy('EmployeeName', 'JobTitle').agg(
    F.concat_ws(',', F.collect_list('Id')).alias('Id(s)')).sort('EmployeeName')

Title_Name_Ids.show()


########## CSV ##########
Title_Name_Ids.coalesce(1).write.csv(
    'Output/Salaries/Title_Name_Ids', header=True, mode='overwrite')

########## Postgres ##########
Title_Name_Ids.write.format('jdbc').options(url='jdbc:postgresql://localhost:5432/Salaries',
                                            driver='org.postgresql.Driver', dbtable='Title_Name_Ids', user='amrit', password='1234').mode('overwrite').save()


##############################################       # 5. Find the number of employees in each job title.                #################################################
Title_Count = Salaries.groupBy('JobTitle').agg(
    F.count('JobTitle').alias('count')).sort('count', ascending=False)

Title_Count.show(5)

print('Highest job title and count:', format(Title_Count.first()))

########## CSV ##########
Title_Count.coalesce(1).write.csv(
    'Output/Salaries/Title_Count', header=True, mode='overwrite')

########## Postgres ##########
Title_Count.write.format('jdbc').options(url='jdbc:postgresql://localhost:5432/Salaries',
                                         driver='org.postgresql.Driver', dbtable='Title_Count', user='amrit', password='1234').mode('overwrite').save()


##############################################      # 6. List out the names and positions of employees whose total pay is greater than 180000.                #################################################
Name_Pos_180k = Salaries.filter(Salaries.TotalPay > 180000).select(
    'EmployeeName', 'JobTitle', 'TotalPay').sort('TotalPay')

Name_Pos_180k.show(5)

########### CSV ###########
Name_Pos_180k.coalesce(1).write.csv(
    'Output/Salaries/Name_Pos_180k', header=True, mode='overwrite')

########### Postgres ###########
Name_Pos_180k.write.format('jdbc').options(url='jdbc:postgresql://localhost:5432/Salaries',
                                           driver='org.postgresql.Driver', dbtable='Name_Pos_180k', user='amrit', password='1234').mode('overwrite').save()


##############################################     # 7. List the names and ids of employees who have never done overtime.                  #################################################

Name_Id_No_OT = Salaries.filter(Salaries.OvertimePay == '0.00').select(
    'EmployeeName', 'Id').sort('EmployeeName')

Name_Id_No_OT.show(5)

print('count: There are {} employees who have never done overtime'.format(
    Name_Id_No_OT.count()))

########### CSV ###########
Name_Id_No_OT.coalesce(1).write.csv(
    'Output/Salaries/Name_Id_No_OT', header=True, mode='overwrite')

########### Postgres ###########
Name_Id_No_OT.write.format('jdbc').options(url='jdbc:postgresql://localhost:5432/Salaries',
                                           driver='org.postgresql.Driver', dbtable='Name_Id_No_OT', user='amrit', password='1234').mode('overwrite').save()
