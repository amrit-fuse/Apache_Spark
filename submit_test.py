

# #################     spark-submit submit_test.py


from pyspark import SparkContext, SparkConf

if not 'sc' in globals():  # This 'trick' makes sure the SparkContext sc is initialized exactly once
    # Spark will use all cores (*) available
    conf = SparkConf().setMaster('local[*]')
    # Initialize SparkContext sc with the above configuration conf
    sc = SparkContext(conf=conf)


from pyspark.sql import SparkSession
# getOrCreate() will return an existing SparkSession if there is one, or create a new one if there is none.
spark = SparkSession.builder.getOrCreate()


# use schema inference to create dataframe i.e sample some data and infer schema from it
flightData2015 = spark.read.csv(
    "Data/2015-summary.csv", header=True, inferSchema=True)

flightData2015.show()
flightData2015.schema
