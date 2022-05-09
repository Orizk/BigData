DATE() {
  date '+%Y-%m-%d %H:%M:%S'
}

echo "[$(DATE)] [Info] [Superset] Starting superset"
superset run --host 0.0.0.0 --port 8088 &
sleep 10
echo "[$(DATE)] [Info] [Superset] Running on http://192.168.33.10:8088/"