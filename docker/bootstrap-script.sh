#!/bin/bash

grep -q '"isMaster": true' /mnt/var/lib/info/instance.json \
|| { echo "Not running on master node, nothing to do" && exit 0; }
# Download and extract Kafka
wget https://downloads.apache.org/kafka/3.6.0/kafka_2.12-3.6.0.tgz
tar -xvf kafka_2.12-3.6.0.tgz

# Start Zookeeper
cd kafka_2.12-3.6.0
bin/zookeeper-server-start.sh config/zookeeper.properties &

# Wait for Zookeeper to start (change the port accordingly)
echo "Waiting for Zookeeper to start..."
while ! nc -z localhost 2181; do
  sleep 1
done
echo "Zookeeper is up and running!"

# Start Kafka server
bin/kafka-server-start.sh config/server.properties &

# Wait for Kafka server to start (change the port accordingly)
echo "Waiting for Kafka server to start..."
while ! nc -z localhost 9092; do
  sleep 1
done
echo "Kafka server is up and running!"

# Create Kafka topic
bin/kafka-topics.sh --create --topic patientvitals --bootstrap-server localhost:9092

# Copy producer script from S3
aws s3 cp s3://patient-vital-monitoring-system/producer.py .

# Install required Python packages
sudo python3 -m pip install kafka-python scipy boto3

# Run the producer script
python3 producer.py
