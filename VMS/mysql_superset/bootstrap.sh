#!/bin/bash

# Functions

DATE() {
  date '+%Y-%m-%d %H:%M:%S'
}

echo "[$(DATE)] [Info] [System] Updating the system..."
apt update &> /dev/null
echo "[$(DATE)] [Info] [System] Installing mysql..."
apt install mysql-server -y &> /dev/null
echo "[$(DATE)] [Info] [MySQL] Setup mysql..."
sed -i 's/bind-address.*/bind-address = 0.0.0.0/g' /etc/mysql/mysql.conf.d/mysqld.cnf
service mysql restart
mysql -e "CREATE USER 'admin'@'%' IDENTIFIED BY 'admin';"
mysql -e "GRANT ALL PRIVILEGES ON * . * TO 'admin'@'%';"
mysql -e "FLUSH PRIVILEGES;"
mysql -e "CREATE DATABASE data;"
echo "[$(DATE)] [Info] [MySQL] mysql user 'admin' created with password 'admin'"
echo "[$(DATE)] [Info] [MySQL] 'data' database created"
echo "[$(DATE)] [Info] [System] Installing dependencies..."
apt install build-essential libssl-dev libffi-dev python3-dev python3-pip libsasl2-dev libldap2-dev default-libmysqlclient-dev -y &> /dev/null
echo "[$(DATE)] [Info] [Superset] Installing apache superset..."
pip install apache-superset &> /dev/null
pip install flask-wtf==0.14.2 &> /dev/null
pip install werkzeug==0.16.1 &> /dev/null
pip install zipp==3.1.0 &> /dev/null
pip install mysqlclient &> /dev/null
echo "[$(DATE)] [Info] [Superset] Setup apache superset..."
superset db upgrade &> /dev/null
superset fab create-admin --username admin --firstname admin --lastname admin --email admin@lecture.fr --password admin &> /dev/null
superset load_examples &> /dev/null
superset init &> /dev/null
