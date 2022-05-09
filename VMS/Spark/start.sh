DATE() {
  date '+%Y-%m-%d %H:%M:%S'
}

echo "[$(DATE)] [Info] [Spark] Starting spark..."
export  SPARK_HOME=/usr/local/spark; \
export PATH=$SPARK_HOME/bin:$PATH; \
export PYSPARK_DRIVER_PYTHON=jupyter; \
export PYSPARK_DRIVER_PYTHON_OPTS='notebook --ip=0.0.0.0 --allow-root'; \
export PATH=$PATH:~/.local/bin; \
pyspark