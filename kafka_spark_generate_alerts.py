from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import to_json, col, current_timestamp, struct

spark=SparkSession.builder.appName("Alerting System").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
in_schema=StructType([StructField("heartBeat",IntegerType()),StructField("bp",IntegerType()),StructField("customerId",IntegerType()),StructField("message_time",TimestampType())])
info_df=spark.read.table("patients_vitals.patients_information")
bp_ref_df=spark.read.table("patients_vitals.threshold_reference").where("alert_flag=1 and attribute='bp'").cache()
heartBeat_ref_df=spark.read.table("patients_vitals.threshold_reference").where("alert_flag=1 and attribute='heartBeat'").cache()
in_df=spark.readStream.format("parquet").schema(in_schema).load("/user/ec2-user/patients_vital_info")

int_df=in_df.withColumn("patientid",in_df["customerId"]).join(info_df,on=["patientid"]).select(["patientname","patientaddress","phone_number","admitted_ward","bp","heartBeat",col("message_time").alias("input_message_time"),"age"]).withColumn("alert_generated_time",current_timestamp())

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
