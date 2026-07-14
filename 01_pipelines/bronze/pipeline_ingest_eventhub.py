from pyspark.sql.functions import col, from_json
from pyspark import pipelines as dp

schema="""
order_id STRING, 
order_timestamp TIMESTAMP, 
restaurant_id STRING, 
customer_id STRING, 
order_type STRING, 
items ARRAY<STRUCT<item_id STRING, name STRING, category STRING, quantity INTEGER, unit_price DOUBLE, subtotal DOUBLE>>, 
total_amount DOUBLE, 
payment_method STRING, 
order_status STRING, 
last_updated TIMESTAMP
"""

@dp.table(name="orders", table_properties={"quality": "bronze"})
def orders():

  # Event Hubs configuration — read inside function to comply with Serverless spark.conf restrictions
  EH_NAMESPACE = dbutils.secrets.get(scope='restaurantproject', key='EVENTHUB_NAMESPACE') 	
  EH_NAME      = dbutils.secrets.get(scope='restaurantproject', key='EVENTHUB_NAME') 	
  EH_CONN_STR  = dbutils.secrets.get(scope='restaurantproject', key='EVENTHUB_CONN_STR') 	

  # Kafka Consumer configuration
  KAFKA_OPTIONS = {
    "kafka.bootstrap.servers"  : f"{EH_NAMESPACE}.servicebus.windows.net:9093",
    "subscribe"                : EH_NAME,
    "kafka.sasl.mechanism"     : "PLAIN",
    "kafka.security.protocol"  : "SASL_SSL",
    "kafka.sasl.jaas.config"   : f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"{EH_CONN_STR}\";",
    "kafka.request.timeout.ms" : "60000",         # Maximum time to wait for a response from Kafka i.e. 60 seconds
    "kafka.session.timeout.ms" : "30000",         # Maximum time to wait for a group to rebalance i.e. 30 seconds
    "maxOffsetsPerTrigger"     : "50000",         # Maximum number of records to read from Kafka in each micro-batch
    "failOnDataLoss"           : "false",         # Continue processing if Event Hubs retention window expires
    "startingOffsets"          : "latest"         # Earliest offset to start reading from Event Hubs
  }

  df_raw = spark.readStream.format("kafka").options(**KAFKA_OPTIONS).load()
  df_parsed = (
    df_raw
    .withColumn("value_str", col("value").cast("string"))
    .withColumn("value_parsed", from_json(col("value_str"), schema))
    .select("value_parsed.*")
    .withColumnRenamed("timestamp", "order_timestamp")
  )
  return df_parsed
