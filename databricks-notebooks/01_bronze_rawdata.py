from pyspark.sql.functions import *
from pyspark.sql.types import *

#Eventhub Configuration
#Eventhub Configuration
EVENTHUBS_NAMESPACE = "<<name-space>>"
EVENT_HUB_NAME="<<hub-name>>"  
event_hub_conn_str ="<<connection-string>>"

kafka_options = {
    'kafka.bootstrap.servers': f"{EVENTHUBS_NAMESPACE}:9093",
    'subscribe': EVENT_HUB_NAME,
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'PLAIN',
    'kafka.sasl.jaas.config': f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{event_hub_conn_str}";',
    'startingOffsets': 'latest',
    'failOnDataLoss': 'false'
}
#Read Data from eventHub
raw_df = (spark.readStream.format("kafka").options(**kafka_options).load())

# Castor convert binary data to rawjson
json_df = raw_df.selectExpr("CAST(value AS STRING) AS raw_json")

spark.conf.set("fs.azure.account.key.<<storageaccount>>.dfs.core.windows.net","storage_account_access_key")

bronze_path = "path"

# write stream to bronze_path
(
    json_df.writeStream.format("delta").outputMode("append").option("checkpointLocation","<<bronze_path>>").start(bronze_path)
)


display(spark.read.format("delta").load(bronze_path))