import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

sc = SparkContext(appName="kafkaTest")
ssc = StreamingContext(sc,5)

print "Connected to spark streaming"

def process(time, rdd):
    print("========= %s =========" % str(time))
    if not rdd.isEmpty():
        rdd.count()
        rdd.first()

ssc = StreamingContext(sc, 5)
kafkaStream = KafkaUtils.createStream(ssc, "server:2181", "pysparkclient1", {"smartPlug": 1})
kafkaStream.pprint()
kafkaStream.foreachRDD(process)

ssc.start()
ssc.awaitTermination()
