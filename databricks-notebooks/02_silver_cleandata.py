from pyspark.sql.functions import *
from pyspark.sql.types import *
#ADLS spark configuration
spark.conf.set("fs.azure.account.key.<<storageaccount>>.dfs.core.windows.net","<<storageacesskey>>")

#Bronze and Silver path
bronze_path = "abfss://<<containername>>@<storageaccount>>.dfs.core.windows.net/patient_flow"
silver_path = "abfss://<<containername>>@<storageaccount>>.dfs.core.windows.net/patient_flow"

# Read Data from Bronze
bronze_df = (spark.readStream.format("delta").load(bronze_path))

#Defining Schema
schema = StructType([
    StructField("patient_id",StringType()),
    StructField("gender",StringType()),
    StructField("age",IntegerType()),
    StructField("department",StringType()),
    StructField("admission_time",StringType()),
    StructField("discharge_time",StringType()),
    StructField("bed_id",IntegerType()),
    StructField("hospital_id",IntegerType()),
])


# Parse it into Dataframe
parse_df = bronze_df.withColumn("data",from_json(col("raw_json"),schema)).select("data.*")

#convert type into timestamp
clean_df = parse_df.withColumn("admission_time", to_timestamp("admission_time"))
clean_df = clean_df.withColumn("discharge_time",to_timestamp("discharge_time"))

#invalid admission_times
clean_df = clean_df.withColumn("admission_time",when(col("admission_time").isNull()|(col("admission_time")>current_timestamp()),
                                                     current_timestamp()).otherwise(col("admission_time")))
#invalid age
clean_df = clean_df.withColumn("age",when(col("age")>100,floor(rand()*90+1).cast("int")).otherwise(col("age")))                                                     

#schema_evolution
expected_columns = ["patient_id","gender","age","department","admission_time","discharge_time","bed_id","hospital_id"]

for col_name in expected_columns:
    if col_name not in clean_df.columns:
        clean_df = clean_df.withColumn(col_name,lit(None))

#write silver_table
(
    clean_df.writeStream.format("delta").outputMode("append").option("mergeSchema","true").option("checkpointLocation",silver_path+"_checkpoint").start(silver_path)
)

display(spark.read.format("delta").load(silver_path))