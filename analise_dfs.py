import sys
import findspark
findspark.init()

from pyspark.sql.functions import when, count, desc, col
from pyspark.sql import SparkSession

def redirect_output_to_file(file_path):
    # Abra o arquivo no modo de gravação
    sys.stdout = open(file_path, 'w')

spark = SparkSession.builder \
    .appName("CassandraIntegration") \
    .config("spark.cassandra.connection.host", "127.0.0.1") \
    .config("spark.cassandra.connection.port", "9042") \
    .getOrCreate()

redirect_output_to_file('logs_analise.txt')

df_movie_100k = spark.read.format("org.apache.spark.sql.cassandra") \
    .options(table="movie_ratings_100k", keyspace="movie_recommendation") \
    .load()

df_high_rated = df_movie_100k.groupBy("movie_id", "movie_name").agg(count(when(df_movie_100k['rating'] == 5, True)).alias("count"))
df_high_rated = df_high_rated.orderBy(desc('count'))
df_high_rated.write.csv("most_probably_recommended_movies.csv", header=True)
print('dataframe com os filmes com avaliação 5, e seu agrupamento pelo movie_id e movie_name')
df_high_rated.show()

df_movie_result = spark.read.format("org.apache.spark.sql.cassandra")\
    .options(table="movie_results", keyspace="movie_recommendation")\
    .load()

df_movie_result_user = df_movie_result.filter((df_movie_result['user_id'] == 1) & (df_movie_result['prediction'] >= 4.5))
print('dataframe com os filmes recomendados para o user#1, que ele avaliaria com 4.5 pelo menos')
df_movie_result_user.show()

join_df = df_movie_result_user.join(df_high_rated, on=["movie_id"], how="inner")
print('dataframe mostrando a quantidade de usuários que avaliaram com 5 naqueles filmes que foram depois recomendados ao user#1')
join_df.show()

if join_df.count() > 0:
    print("Existem valores comuns nos DataFrames:", join_df.count())
else:
    print("Não existem valores comuns nos DataFrames.")

spark.stop()
sys.stdout.close()
sys.stdout = sys.__stdout__