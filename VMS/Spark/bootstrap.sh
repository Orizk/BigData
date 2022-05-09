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
# Setup python3
echo "[$(DATE)] [Info] [Python] Setup python3..."
rm -rf /usr/bin/python
ln -s /usr/bin/python3 /usr/bin/python
echo "[$(DATE)] [Info] [Python] Installing pip3..."
apt install -y python3-pip &> /dev/null
echo "[$(DATE)] [Info] [Python] Installing jupyter..."
pip3 install jupyter &> /dev/null
# Install Spark
echo "[$(DATE)] [Info] [Spark] Installing spark..."
wget https://downloads.apache.org/spark/spark-2.4.8/spark-2.4.8-bin-hadoop2.7.tgz &> /dev/null
tar -xzf spark-2.4.8-bin-hadoop2.7.tgz
mv spark-2.4.8-bin-hadoop2.7 /usr/local/spark