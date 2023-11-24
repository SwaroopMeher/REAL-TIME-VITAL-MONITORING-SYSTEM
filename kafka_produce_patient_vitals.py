from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType
from pyspark.sql.functions import col, expr

# Create a SparkSession
spark = SparkSession.builder.appName("KafkaProducer").getOrCreate()

# Define the schema for the CSV file
schema = StructType([
    StructField("patientId", IntegerType(), True),
    StructField("heartBeat", FloatType(), True),
    StructField("systolicBP", FloatType(), True),
    StructField("diastolicBP", FloatType(), True),
    StructField("temperature", FloatType(), True),
    StructField("respirationRate", FloatType(), True),
    StructField("spO2", FloatType(), True)
])

# Read data from the CSV file
df_stream = spark.readStream \
    .format("csv") \
    .option("header", True) \
    .schema(schema) \
    .option("path", "patient_vitals_CSV") \
    .load()

df_stream = df_stream.withColumn("timestamp", expr("current_timestamp()"))

query = df_stream.selectExpr("CAST(null AS STRING) AS key", "to_json(struct(*)) AS value")

# Write the transformed data to a Kafka topic
query.writeStream \
    .format("kafka") \
    .outputMode("append") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "Patients-Vitals-Info") \
    .option("startingOffsets", "earliest") \
    .option("checkpointLocation", "./kafka_checkpoint/produce_patient_vitals/") \
    .start() \
    .awaitTermination()

# Stop the SparkSession
spark.stop()
