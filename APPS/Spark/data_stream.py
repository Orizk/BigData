import string
from pyspark.sql import SparkSession, Row
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.types import FloatType, StringType
from pyspark.sql.functions import to_date




def process(time, rdd):
    print("========= %s =========" % str(time))
    if not rdd.isEmpty():
        df = rdd.map(lambda line: Row(date=line[0], 
                              number=line[1], 
                              country_code=line[2])).toDF()

        df = df.withColumn("date", to_date(df["date"], 'dd/MM/yyyy')) \
           .withColumn("number", df["number"].cast(FloatType())) \
           .withColumn("country_code",df["country_code"].cast(StringType()))

        df.write.format('jdbc').options(
            url='jdbc:mysql://192.168.33.10/data',
            dbtable='mydata_table',
            user='admin',
            password='admin').mode('append').save()

spark = SparkSession.builder \
        .master("local[2]") \
        .appName("mydemo_data") \
        .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc, 10)

directKafkaStream = KafkaUtils.createDirectStream(ssc, ["mydemo_data_topic"], {"metadata.broker.list": "192.168.33.13:9092"})

rdd = directKafkaStream.map(lambda line: line[1].split(","))
rdd.foreachRDD(process)

ssc.start()
ssc.awaitTermination()