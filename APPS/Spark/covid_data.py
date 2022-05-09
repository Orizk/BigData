import string
from pyspark.sql import SparkSession, Row
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.types import FloatType, StringType, IntegerType
from pyspark.sql.functions import to_date


def process(time, rdd):
    print("========= %s =========" % str(time))
    if not rdd.isEmpty():
        df = rdd.map(lambda line: Row(date=line[0], 
                              todaycase=line[4],
                              deaths=line[5],
                              country_name=line[6],
                              country_ID=line[7],       
                              country_Code=line[8],       
                              population=line[9],
                              continent=line[10])).toDF()

        df = df.withColumn("date", to_date(df["date"], 'dd/MM/yyyy')) \
            .withColumn("todaycase", df["todaycase"].cast(IntegerType())) \
            .withColumn("deaths", df["deaths"].cast(IntegerType())) \
            .withColumn("country_name", df["country_name"].cast(StringType())) \
            .withColumn("country_ID", df["country_ID"].cast(StringType())) \
            .withColumn("country_Code", df["country_Code"].cast(StringType())) \
            .withColumn("population", df["population"].cast(IntegerType())) \
            .withColumn("continent",df["continent"].cast(StringType()))

        df.write.format('jdbc').options(
            url='jdbc:mysql://192.168.33.10/data',
            dbtable='covid_table',
            user='admin',
            password='admin').mode('append').save()

spark = SparkSession.builder \
        .master("local[2]") \
        .appName("mydemo_covid_data") \
        .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc, 10)

directKafkaStream = KafkaUtils.createDirectStream(ssc, ["covid"], {"metadata.broker.list": "192.168.33.13:9092"})

rdd = directKafkaStream.map(lambda line: line[1].split(","))
rdd.foreachRDD(process)

ssc.start()
ssc.awaitTermination()
