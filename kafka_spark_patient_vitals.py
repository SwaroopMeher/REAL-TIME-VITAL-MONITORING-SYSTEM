from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import from_json, col

spark=SparkSession.builder.appName("Kafka Topic Patient Vitals").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

schem=StructType([StructField("heartBeat",IntegerType()),StructField("customerId",IntegerType()),StructField("bp",IntegerType())])

in_df=spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "Patients-Vitals-Info").option("startingOffsets", "earliest").load()
in_df1 = in_df.selectExpr(["CAST(value AS STRING)", "timestamp"])
in_df2 = in_df1.select([from_json(col("value"), schem).alias("data"),col("timestamp").alias("message_time")]).select(["data.*","message_time"])
in_df2.writeStream \
      .format("parquet") \
      .outputMode("append") \
      .option("path","/user/ec2-user/patients_vital_info") \
      .option("checkpointLocation","/user/ec2-user/checkpointlocation") \
      .option("parquet.block.size", 1024) \
      .start() \
      .awaitTermination()
