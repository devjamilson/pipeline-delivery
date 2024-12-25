from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('leitura').getOrCreate()

df = spark.read.parquet('/home/jamilsonfs/pipeline/visualizacao/dados/pedidos')
df.show()