from pyspark.sql import SparkSession

MAX_MEMORY = "8g"

working_directory = 'jars/*'
mongodb_uri = "mongodb://<user-name>:<password>@<ip-address>"
spark = SparkSession \
    .builder \
    .appName("mongo") \
    .config('spark.driver.extraClassPath', working_directory) \
    .config("spark.mongodb.input.uri",mongodb_uri ) \
    .config("spark.mongodb.output.uri", mongodb_uri).config("spark.executor.memory", MAX_MEMORY) \
    .config('spark.driver.memory','8g').config('spark.cores.max', '3').getOrCreate()

df = spark.read.format("mongo").option("database","<db-name>").option("collection", "<collection-name>").load()

df.write.format("mongo").options(uri="mongodb://<user-name>:<password>@<ip-address>/?authSource=admin")\
    .option("database","<db-name>").option("collection", "<collection-name>").mode("append").save()
