from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType
from pyspark.sql.functions import col

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
    .option("path", "patient_vitals_CSV/patient_vitals*.csv") \
    .option("rateLimit", 1) \
    .load()

# Write the transformed data to a Kafka topic
df_stream.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "Patients-Vitals-Info") \
    .option("startingOffsets", "earliest") \
    .option("checkpointLocation", "./kafka_checkpoint/produce_patient_vitals/") \
    .start() \
    .awaitTermination()

# Stop the SparkSession
spark.stop()
