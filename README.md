<h1>Recomendação de Filmes (Pyspark & Cassandra) - ACH2066 </h1>
  <h4>Aplicação para a matéria ACH2066 - Tópicos Especiais em Banco de Dados</h4>
  <h4>Integrantes</h4>
  <ul>
    <li>Bruno de Sousa Almeida - 9911451</li>
    <li>Fábio Kiyoshi Ichimura - 10687581</li>
    <li>Felipe Munhos Escobar - 11795620</li>
    <li>Gustavo Henrique Barbosa - 11857351</li>
    <li>Vitor Contieri Rezende Peçanha - 10387706</li>
    <li>Wallace Ramon Nogueira Soares - 11847030</li>
  </ul>
  <hr>
  <h4>Informações gerais</h4>
  
    - Ubuntu 22.04
    - Apache Spark 3.3.2
	- Apache Spark 3.3.2	
		- Instalar o Apache Spark 3.3.2 - https://spark.apache.org/downloads.html
		- Como instalar e configurar o Spark(e.g variáveis de ambiente como $SPARK_HOME) no ubuntu 		22.04- https://computingforgeeks.com/how-to-install-apache-spark-on-ubuntu-debian/
		- Depois de instalar e configurar para rodar o PySpark somente rodar 'pyspark'
	- Apache Cassandra 4.1.2
		- Instalar o Cassandra 4.1.2 - https://www.hostinger.com/tutorials/set-up-and-install-cassandra-ubuntu/
		- Configurar o $CASSANDRA_HOME:
			- abrir um terminal e digitar: 'which cassandra'
			- a saida pode ser algo como '/usr/bin/cassandra'
			- após isto, 'nano ~/.bashrc' no terminal
				- isto irá abrir um arquivo e com isso adicione a seguinte linha:
					- export CASSANDRA_HOME=/caminho/para/o/cassandra
					- na qual o /caminho/para/o/cassandra é o diretorio retornado após o comando 'which cassandra'
				- salvar e sair
			- execute o comando 'source ~/.bashrc' para recarregar o arquivo de configuração
	- Configurar as propriedades de conexão do Cassandra:
		- no arquivo 'spark-default.conf'(localizado no diretório de configuração do Spark) adicionar as seguintes linhas:
			spark.jars.packages com.datastax.spark:spark-cassandra-connector_2.12:3.3.0
			spark.cassandra.connection.host <endereço IP do Cassandra>
			spark.cassandra.connection.port <porta do Cassandra>
		- substituir <endereço IP do Cassandra> pelo IP configurado na instalação do Cassandra(geralmente 127.0.0.1)
		- substituir <porta do Cassandra> pela porta configurada na instalação do Cassandra(geralmente 9042)
  <hr>
  <h4>Comandos Cassandra</h4>
  <b>Para criar um keyspace</b>:
  	-> CREATE KEYSPACE movie_recommendations WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};
  <br>
   <b>Para listar todos os keyspaces</b>
  	-> DESC KEYSPACES;
  <br>
  <b>Para usar um keyspace</b>
   	-> USE KEYSPACE <nome_keyspace>;
   <br>
  <b>Para criar tabelas</b>
  	->CREATE TABLE movie_ratings_100k (
	partition_column int,
	user_id int,
	movie_id int,
	rating int,
	movie_name text,
	PRIMARY KEY(partition_column)
	);
	->CREATE TABLE movie_results (
	user_id int,
	movie_id int,
	prediction float,
	PRIMARY KEY(prediction)
	);
  <b>Para importar dados de um CSV</b>
	-> COPY movie_recommendation.movie_ratings_100k FROM '<pasta-com-os-dados>' WITH DELIMITER=',' AND HEADER=TRUE;
  <h4>Alguns links úteis</h4>
  <ul>
    <li>https://github.com/edersoncorbari/movie-rec</li>
    <li>https://github.com/microsoft/recommenders/blob/main/examples/02_model_collaborative_filtering/als_deep_dive.ipynb</li>
    <li>https://github.com/liambll/yelp-recommender-system</li>
    <li>Onde foi retirado o dataset dos filmes:
      https://grouplens.org/datasets/movielens/<li>
  </ul>
  
  
