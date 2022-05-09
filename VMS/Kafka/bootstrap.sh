#!/bin/bash

# Functions

DATE() {
  date '+%Y-%m-%d %H:%M:%S'
}

# Let's go
# Update the System
echo "[$(DATE)] [Info] [System] Updating the system..."
apt update &> /dev/null
# Install Java
if [ $(dpkg-query -W -f='${Status}' openjdk-8-jdk 2>/dev/null | grep -c "ok installed") -eq 0 ];
then
  echo "[$(DATE)] [Info] [Java] Installing Java..."
  add-apt-repository -y ppa:openjdk-r/ppa &> /dev/null
  apt update &> /dev/null
  apt -y install openjdk-8-jdk &> /dev/null
fi

# Install Kafka
echo "[$(DATE)] [Info] [Kafka] Installing Kafka..."
wget https://archive.apache.org/dist/kafka/2.8.0/kafka_2.13-2.8.0.tgz &> /dev/null
tar -xzf kafka_2.13-2.8.0.tgz
mv kafka_2.13-2.8.0 kafka

# Configure Kafka
echo "[$(DATE)] [Info] [Kafka] Configure Kafka..."
sed -i 's/#listeners=PLAINTEXT:\/\/:9092/listeners=PLAINTEXT:\/\/192.168.33.13:9092/g' kafka/config/server.properties
chown -R vagrant:vagrant kafka