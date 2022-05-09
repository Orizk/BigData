import string
from pyspark.sql import SparkSession, Row
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.types import FloatType, StringType, IntegerType
from pyspark.sql.functions import to_date, to_timestamp




def process(time, rdd):
    print("========= %s =========" % str(time))
    if not rdd.isEmpty():
        df = rdd.map(lambda line: Row(ville=line[0], 
                              codepostal=line[1],
                              current_temperature=line[2],
                              hourly_temperature=line[3],
                              hourly_time=line[4])).toDF()
        
        df = df.withColumn("ville", df["ville"].cast(StringType())) \
            .withColumn("codepostal", df["codepostal"].cast(StringType())) \
            .withColumn("current_temperature", df["current_temperature"].cast(FloatType())) \
            .withColumn("hourly_temperature",df["hourly_temperature"].cast(FloatType())) \
            .withColumn("hourly_time", to_timestamp(df["hourly_time"],"yyyy-MM-dd HH:mm"))
        
        df.write.format('jdbc').options(
            url='jdbc:mysql://192.168.33.10/data',
            dbtable='weather_data',
            user='admin',
            password='admin').mode('append').save()


spark = SparkSession.builder \
        .master("local[2]") \
        .appName("mydemo_data") \
        .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc, 10)

directKafkaStream = KafkaUtils.createDirectStream(ssc, ["weather"], {"metadata.broker.list": "192.168.33.13:9092"})

rdd = directKafkaStream.map(lambda line: line[1].split(","))
rdd.foreachRDD(process)

ssc.start()
ssc.awaitTermination()