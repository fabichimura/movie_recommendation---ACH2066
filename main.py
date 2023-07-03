import sys
import time
import findspark
findspark.init()

from pyspark.sql.functions import col
from pyspark.sql.types import FloatType
from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator

#Colunas da tabela do Cassandra
COL_USER_ID = 'user_id'
COL_MOVIE_ID = 'movie_id'
COL_MOVIE_NAME = 'movie_name'
COL_RATING = 'rating'

#ALS params
RANK = 20
MAX_ITER = 18
REG_PARAM = 0.1

def redirect_output_to_file(file_path):
    # Abra o arquivo no modo de gravação
    sys.stdout = open(file_path, 'w')


spark = SparkSession.builder \
    .appName("CassandraIntegration") \
    .config("spark.cassandra.connection.host", "127.0.0.1") \
    .config("spark.cassandra.connection.port", "9042") \
    .getOrCreate()

redirect_output_to_file('logs_main.txt')

df_cassandra = spark.read.format("org.apache.spark.sql.cassandra") \
    .options(table="movie_ratings_100k", keyspace="movie_recommendation") \
    .load()

df = df_cassandra.withColumn(COL_RATING, col(COL_RATING).cast(FloatType()))
print('dataframe vindo da tabela do cassandra:')
df.describe().show()

# Dividir o DataFrame em conjuntos de treinamento e validação
train_df, val_df = df.randomSplit([0.8, 0.2], seed=42)
print('N dataframe:',df.count())
print ("N treino:", train_df.count())
print ("N teste:", val_df.count())

# Treinamento do modelo
als = ALS(
    rank=RANK,
    maxIter=MAX_ITER,
    implicitPrefs=False,
    regParam=REG_PARAM,
    coldStartStrategy='drop',
    nonnegative=True,
    userCol=COL_USER_ID,
    itemCol=COL_MOVIE_ID,
    ratingCol=COL_RATING
)

start_train_time = time.time()
model = als.fit(train_df)
end_train_time = time.time()
training_time = end_train_time - start_train_time
print('Tempo de treinamento: {} segundos'.format(training_time))

# validando o modelo
start_val_time = time.time()
# cross join de todos os usuarios/filmes e aplica um score a estes
users = train_df.select(COL_USER_ID).distinct()
movies = train_df.select(COL_MOVIE_ID).distinct()
user_movies = users.crossJoin(movies)
df_pred = model.transform(user_movies)


print('dataframe logo após a validacao cruzada')
df_pred.show(15)

filtered_df = df_pred.filter(df_pred[COL_USER_ID] == 1) 
filtered_df.show()

df_pred_alias = df_pred.alias('pred')
train_df_alias = train_df.alias('train')

df_pred_exclude_train = df_pred_alias.join(
    train_df_alias,
    (df_pred_alias[COL_USER_ID] == train_df_alias[COL_USER_ID]) & (df_pred_alias[COL_MOVIE_ID] == train_df_alias[COL_MOVIE_ID]),
    how='outer'
)

df_pred_final = df_pred_exclude_train.filter(df_pred_exclude_train[f"train.{COL_RATING}"].isNull()) \
    .select('pred.' + COL_USER_ID, 'pred.' + COL_MOVIE_ID, 'pred.' + "prediction")


end_val_time = time.time()
val_time = end_val_time - start_val_time
print("Tempo para predição: {} segundos.".format(val_time))

print('dataframe final com o que será salvo na tabela do cassandra')
df_pred_final.show(15)
print('desc do dataframe final')
df_pred_final.describe().show()


df_pred_final.write \
  .format("org.apache.spark.sql.cassandra") \
  .options(table="movie_results", keyspace="movie_recommendation") \
  .mode("append") \
  .save()


filtered_df_final = df_pred_final.filter(df_pred_final[COL_USER_ID] == 1) 
filtered_df_final.show()

df_pred_final = df_pred_final.join(
    df,
    on=[COL_USER_ID, COL_MOVIE_ID],
    how='inner'
)

result_df = df_pred_final.select(col(COL_USER_ID), col(COL_MOVIE_ID), col(COL_RATING), col('prediction'))
filtered_df_result = result_df.filter(result_df[COL_USER_ID] == 1)
print('dataframe mostrando apenas para o user_id=1')
filtered_df_result.show()



# Cria um objeto RankingEvaluator
evaluator = RegressionEvaluator(labelCol=COL_RATING, predictionCol="prediction")

# Calcule as métricas usando o conjunto de teste e as previsões
rmse = evaluator.evaluate(df_pred_final, {evaluator.metricName: "rmse"})
r_squared = evaluator.evaluate(df_pred_final, {evaluator.metricName: "r2"})
mae = evaluator.evaluate(df_pred_final, {evaluator.metricName: "mae"})


print("Model:\tALS rating prediction",
      "RMSE:\t%f" % rmse,
      "R squared:\t%f" % r_squared,
      "mae:\t%f" % mae,sep='\n'
      )

spark.stop()
sys.stdout.close()
sys.stdout = sys.__stdout__











