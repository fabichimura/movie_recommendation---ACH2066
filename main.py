import findspark
findspark.init()

from pyspark.sql import SparkSession


RANK = 10
MAX_ITER = 15
REG_PARAM = 0.05
K = 10

spark = SparkSession.builder \
    .appName("CassandraIntegration") \
    .config("spark.cassandra.connection.host", "127.0.0.1") \
    .config("spark.cassandra.connection.port", "9042") \
    .getOrCreate()

df = spark.read.format("org.apache.spark.sql.cassandra") \
    .options(table="udata", keyspace="movie_recommendation") \
    .load()

df.show()
