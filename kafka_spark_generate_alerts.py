from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import to_json, col, current_timestamp, struct

spark=SparkSession.builder.appName("Alerting System").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

in_schema = StructType([
       StructField("patientId", IntegerType(), True),
       StructField("heartBeat", FloatType(), True),
       StructField("systolicBP", FloatType(), True),
       StructField("diastolicBP", FloatType(), True),
       StructField("temperature", FloatType(), True),
       StructField("respirationRate", FloatType(), True),
       StructField("spO2", FloatType(), True),
       StructField("timestamp", TimestampType(), False)
])

info_df = spark.read.csv("patient_vitals_CSV/patient_info.csv")

threshold_schema = StructType([
       StructField("Metric", StringType(), False),
       StructField("Normal_low_val", FloatType(), False),
       StructField("Normal_high_val", FloatType(), False)
])

threshold_df = spark.read.csv("threshold_values.csv", schema=threshold_schema)

vital_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "Patients-Vitals-Info") \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .selectExpr("from_json(value, 'your_json_schema') as data") \
    .select("data.*")



int_df = vital_df.withColumn("patientid",in_df["customerId"]).join(info_df,on=["patientid"]).select(["patientname","patientaddress","phone_number","admitted_ward","bp","heartBeat",col("message_time").alias("input_message_time"),"age"]).withColumn("alert_generated_time",current_timestamp())

bp_trigger_df=int_df.crossJoin(bp_ref_df).where("bp>=low_value and bp<=high_value and age>=low_age_limit and age<=high_age_limit")
heartBeat_df=int_df.crossJoin(heartBeat_ref_df).where("heartBeat>=low_value and heartBeat<=high_value and age>=low_age_limit and age<=high_age_limit")
final_df=bp_trigger_df.unionAll(heartBeat_df).select(["patientname","age","patientaddress","phone_number","admitted_ward","bp","heartBeat","input_message_time","alert_generated_time","alert_message"])


final_df.selectExpr("to_json(struct(*)) AS value") \
       .writeStream \
       .format("kafka") \
       .option("kafka.bootstrap.servers", "localhost:9092") \
       .option("topic", "Alert-Messages") \
       .option("checkpointLocation","/user/ec2-user/trgt_checkpointlocation") \
       .trigger(processingTime='5 seconds') \
       .outputMode("append") \
       .start() \
       .awaitTermination()
