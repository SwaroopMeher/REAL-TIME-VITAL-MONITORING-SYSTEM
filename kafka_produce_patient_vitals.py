from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType
from pyspark.sql.functions import col

# Create a SparkSession
spark = SparkSession.builder.appName("KafkaProducer").getOrCreate()

# Define the schema for the CSV file
schema = StructType([
    StructField("customerId", IntegerType(), True),
    StructField("heartBeat", FloatType(), True),
    StructField("systolicBP", FloatType(), True),
    StructField("diastolicBP", FloatType(), True),
    StructField("temperature", FloatType(), True),
    StructField("respirationRate", FloatType(), True),
    StructField("spO2", FloatType(), True)
])

# Read data from the CSV file
df = spark.read.csv("patient_vitals.csv.csv", header=True, schema=schema)

# Transform the data
df_transformed = df.selectExpr(
    "customerId",
    "heartBeat",
    "systolicBP",
    "diastolicBP",
    "temperature",
    "respirationRate",
    "spO2"
)

# Write the transformed data to a Kafka topic
df_transformed.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "Patients-Vitals-Info") \
    .option("startingOffsets", "earliest") \
    .start() \
    .awaitTermination()

# Stop the SparkSession
spark.stop()
