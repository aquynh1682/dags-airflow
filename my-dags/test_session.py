import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType

os.environ["PYSPARK_SUBMIT_ARGS"] = "--master %s pyspark-shell"%("spark://spark-cluster-hduser:7077")

spark = SparkSession \
        .builder \
        .appName("Spark Session 1") \
        .master("spark://spark-cluster-hduser:7077") \
        .config("deploy.mode", "cluster") \
        .config("spark.driver.cores", 2) \
        .config("spark.driver.memory", "4G") \
        .config("spark.executor.cores", 1) \
        .config("spark.executor.memory", "4G")\
        .getOrCreate()

data2 = [("James","","Smith","36636","M",3000),
    ("Michael","Rose","","40288","M",4000),
    ("Robert","","Williams","42114","M",4000),
    ("Maria","Anne","Jones","39192","F",4000),
    ("Jen","Mary","Brown","","F",-1)
  ]

schema = StructType([ \
    StructField("firstname",StringType(),True), \
    StructField("middlename",StringType(),True), \
    StructField("lastname",StringType(),True), \
    StructField("id", StringType(), True), \
    StructField("gender", StringType(), True), \
    StructField("salary", IntegerType(), True) \
  ])
 
df = spark.createDataFrame(data=data2,schema=schema)
df.printSchema()
df.show(truncate=False)
