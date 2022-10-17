# spark-submit joins.py
# bash start-history-server.sh
# http://localhost:18080

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Joins")\
    .config("spark.eventLog.enabled", "true")\
    .config("spark.eventLog.logDirectory", "tmp/spark-events")\
    .getOrCreate()

# if error of tmp/spark-events not found, create it manually


emp = [(1, "John", "2018", "10", "M", 3000),

       (2, "Dario", "2010", "20", "M", 4000),

       (3, "Ross", "2010", "10", "M", 1000),

       (4, "Rachel", "2005", "40", "F", 2000),

       (5, "Monica", "2010", "50", "F", 3000),

       ]

empColumns = ["emp_id", "name", "year_joined",

              "emp_dept_id", "gender", "salary"]  # Schema for the employee data

empDF = spark.createDataFrame(data=emp, schema=empColumns)

empDF.printSchema()

empDF.show(truncate=False)

dept = [("Finance", 10),

        ("Marketing", 20),

        ("Sales", 30),

        ("IT", 40)

        ]

deptColumns = ["dept_name", "dept_id"]  # Schema for the department data

deptDF = spark.createDataFrame(data=dept, schema=deptColumns)

deptDF.printSchema()

deptDF.show(truncate=False)


empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, "inner").show()

empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, "left").show()

empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, "leftouter").show()


empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, "right").show()

empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, "rightOuter").show()

empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, "outer").show()


empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, "full").show()


empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, "fullOuter").show()


empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, "leftsemi").show()
# left semi join returns only the left table rows that have a match in the right table

empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, "leftanti").show()
# left anti join returns only the left table rows that do not have a match in the right table
