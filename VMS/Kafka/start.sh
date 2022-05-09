DATE() {
  date '+%Y-%m-%d %H:%M:%S'
}
echo "[$(DATE)] [Info] [Kafka] Starting Zookeeper..."
kafka/bin/zookeeper-server-start.sh kafka/config/zookeeper.properties & &> /dev/null
echo "[$(DATE)] [Info] [Kafka] Starting Kafka..."
kafka/bin/kafka-server-start.sh kafka/config/server.properties & &> /dev/null