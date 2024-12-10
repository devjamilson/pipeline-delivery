from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql import DataFrame

from ETL.extract import Extractor
from ETL.transform import Transformer
from ETL.load import Loader

spark = SparkSession.builder.appName("pipeline").getOrCreate()

"""
Extraindo os dados
"""
payments = Extractor('dados/payments.csv', spark)
payments.load_data()
channels = Extractor('dados/channels.csv', spark)
channels.load_data()
deliveries = Extractor('dados/deliveries.csv', spark)
deliveries.load_data()
drivers = Extractor('dados/drivers.csv', spark)
drivers.load_data()
hubs = Extractor('dados/hubs.csv', spark)
hubs.load_data()
orders = Extractor('dados/orders.csv', spark)
orders.load_data()
stores = Extractor('dados/stores.csv', spark)
stores.load_data()


"""
Transformando os dados
"""
payments_transforme = Transformer(payments.data)
channels_transforme = Transformer(channels.data)
deliveries_transforme = Transformer(deliveries.data)
drivers_transforme = Transformer(drivers.data)
hubs_transforme = Transformer(hubs.data)
orders_transforme = Transformer(orders.data)
stores_transforme = Transformer(stores.data)

#transforme: unificando os dados 
orders_transforme.join(payments_transforme.data, on_column='payment_order_id')
orders_transforme.show_data()