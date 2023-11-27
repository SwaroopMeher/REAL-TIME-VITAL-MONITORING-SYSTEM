# consumer.py
#from kafka import KafkaConsumer
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, expr
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType
from pyspark.sql.functions import col
import functools
import json
import os
import boto3

redshift = boto3.client("redshift-data", region_name="us-east-1")
sns = boto3.client('sns',region_name='us-east-1')
secret_arn = 'arn:aws:iam::903187628715:role/myRedshiftRole' #redshift
snsTopicArn = 'arn:aws:sns:us-east-1:903187628715:patient_vital_alert' #sns

threshold_values = {
    'Heart Rate': (50,100),
    'Systolic BP': (50, 140),
    'Diastolic BP': (50, 90),
    'Temperature': (90,98.6),
    'Respiration Rate': (8, 20),
    'SpO2': (89,101)
}

if __name__ == "__main__":
    spark = SparkSession.builder.appName("VitalsAlerts")\
        .getOrCreate()
    # docker 
    # kafka_bootstrap_servers = "kafka:9093"
    # Cluster
    kafka_bootstrap_servers = "localhost:9092"
    kafka_topic = "patientvitals"
    spark.sparkContext.setLogLevel('WARN')
    # Read data from Kafka using the readStream API
    kafka_stream_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "latest")\
        .load()
    
    schema = StructType([
    StructField("Patient ID", IntegerType(), True),
    StructField("Heart Rate", IntegerType(), True),
    StructField("Systolic BP", IntegerType(), True),
    StructField("Diastolic BP", IntegerType(), True),
    StructField("Temperature", FloatType(), True),
    StructField("Respiration Rate", IntegerType(), True),
    StructField("SpO2", FloatType(), True)
    ])

    #value_df = kafka_stream_df.selectExpr("CAST(value AS STRING)")
    json_df = kafka_stream_df.select(from_json(col("value").cast("string"), schema).alias("value")).select("value.*")

    # Write the streaming result to the console
    # query = json_df.writeStream.outputMode("append").format("console").start()

    # query.awaitTermination()

    def load_stream(records):
        for row in records.toLocalIterator():
            # insert the row the main table
            redshift_query = f"""
            INSERT INTO public.patient_vitals
            (patient_id, heart_rate, systolic_bp, diastolic_bp, temperature, respiration_rate, spo2)
            VALUES
            ({row["Patient ID"]}, {row["Heart Rate"]}, {row["Systolic BP"]}, {row["Diastolic BP"]}, {row["Temperature"]}, {row["Respiration Rate"]}, {row["SpO2"]})"""
            #redshift.execute(redshift_query)
            clientdata.execute_statement(
                ClusterIdentifier='redshift-cluster-1',
                Database='dev',
                DbUser='awsuser',
                Sql=redshift_query,
                StatementName='InsertData',
                SecretArn=secret_arn
            )

    # Check for threshold crossing and print alerts
    def process_alerts(records):
        for row in records.toLocalIterator():
            get_info = True          
            for column in threshold_values.keys():
                alert_column = f"{column}_alert"
                if row[alert_column] == 1:
                    alert_query = f"""
                    INSERT INTO public.alerts (patient_id, alert_metric, value, threshold_range)
                    VALUES
                    ({row["Patient ID"]}, '{column}', {row[column]}, '{threshold_values[column][0]}-{threshold_values[column][1]}') """
                    #redshift.execute(alert_query)
                    clientdata.execute_statement(
                        ClusterIdentifier='redshift-cluster-1',
                        Database='dev',
                        DbUser='awsuser',
                        Sql=alert_query,
                        StatementName='InsertQueryData',
                        SecretArn=secret_arn
                    )
                    
                    #sns
                    #print(f"Alert: {column} crossed the threshold. Current value: {row[column]}, Threshold: {threshold_values[column]}")
                    if get_info:
                        # Execute the SELECT query
                        select_query = f"SELECT patient_name, address FROM public.patients WHERE patient_id = {row['Patient ID']};"
                        response = clientdata.execute_statement(
                            ClusterIdentifier='redshift-cluster-1',
                            Database='dev',
                            DbUser='awsuser',
                            Sql=select_query,
                            StatementName='SelectData',
                            SecretArn=secret_arn
                        )

                        result = clientdata.get_statement_result(
                                Id=response['Id'])
                        
                        for row in result['Records']:
                            patient_name = row[0]['stringValue']
                            address = row[1]['stringValue']
                        get_info=False

                    alert = f"Alert: Patient: {patient_name},\nAddress: {address},\n{column} is abnormal.\nCurrent value: {row[column]}\nMust be in: {threshold_values[column]}"
                    response = sns.publish(
                                TopicArn=snsTopicArn,
                                Message=str(alert))
    
    for column, (min_threshold, max_threshold) in threshold_values.items():
        json_df = json_df.withColumn(f"{column}_alert", expr(f"IF(`{column}` < {min_threshold} OR `{column}` > {max_threshold}, 1, 0)"))

    # Filter the DataFrame to include only rows where any alert column is 1
    filtered_df = json_df.filter(expr(" OR ".join([f"`{column}_alert` = 1" for column in threshold_values.keys()])))

    # Define the streaming query and output the results
    query_load = json_df \
        .writeStream \
        .outputMode("append") \
        .foreachBatch(lambda batch_df, batch_id: load_stream(batch_df)) \
        .start()

    query_alert = filtered_df \
        .writeStream \
        .outputMode("append") \
        .foreachBatch(lambda batch_df, batch_id: process_alerts(batch_df)) \
        .start()

    query_load.awaitTermination()
    query_alert.awaitTermination()

#---------------------------Using Kafka-python-------------------------------------

# consumer = KafkaConsumer(
#     'patient_vitals',
#     bootstrap_servers='localhost:9092',
#     auto_offset_reset='earliest',
#     group_id='my-group',
#     value_deserializer=lambda x: json.loads(x.decode('utf-8'))
# )

# if __name__ == "__main__":
#     for message in consumer:
#         vitals = message.value
#         print("Received Vitals:", vitals)
#         check_thresholds(vitals)