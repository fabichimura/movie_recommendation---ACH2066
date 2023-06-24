import time
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator


#Colunas da tabela do Cassandra
COL_USER_ID = 'user_id'
COL_MOVIE_ID = 'movie_id'
COL_MOVIE_NAME = 'movie_name'
COL_RATING = 'rating'

#ALS params
RANK = 10
MAX_ITER = 15
REG_PARAM = 0.05
K = 10

spark = SparkSession.builder \
    .appName("CassandraIntegration") \
    .config("spark.cassandra.connection.host", "127.0.0.1") \
    .config("spark.cassandra.connection.port", "9042") \
    .getOrCreate()


df_cassandra = spark.read.format("org.apache.spark.sql.cassandra") \
    .options(table="movie_ratings_100k", keyspace="movie_recommendation") \
    .load()


df = df_cassandra.select(COL_USER_ID, COL_MOVIE_ID, COL_RATING)

# Dividir o DataFrame em conjuntos de treinamento e validação
train_df, val_df = df.randomSplit([0.8, 0.2], seed=42)
print('df count',df.count())
print ("N train", train_df.count())
print ("N test", val_df.count())


# Treinamento do modelo
als = ALS(
    rank=RANK,
    maxIter=MAX_ITER,
    implicitPrefs=False,
    regParam=REG_PARAM,
    coldStartStrategy='drop',
    nonnegative=False,
    seed=42,
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
df_pred = model.transform(val_df)

# cross join de todos os usuarios/filmes e aplica um score a estes
users = train_df.select(COL_USER_ID).distinct()
movies = train_df.select(COL_MOVIE_ID).distinct()
user_movies = users.crossJoin(movies)
df_pred = model.transform(user_movies)


df_pred_alias = df_pred.alias('pred')
train_df_alias = train_df.alias('train')

df_pred_exclude_train = df_pred_alias.join(
    train_df_alias,
    (df_pred_alias[COL_USER_ID] == train_df_alias[COL_USER_ID]) & (df_pred_alias[COL_MOVIE_ID] == train_df_alias[COL_MOVIE_ID]),
    how='outer'
)

top_all = df_pred_exclude_train.filter(df_pred_exclude_train[f"train.{COL_RATING}"].isNull()) \
    .select('pred.' + COL_USER_ID, 'pred.' + COL_MOVIE_ID, 'pred.' + "prediction")


top_all.cache().count()

end_val_time = time.time()
val_time = end_val_time - start_val_time
print("Took {} seconds for prediction.".format(val_time))













