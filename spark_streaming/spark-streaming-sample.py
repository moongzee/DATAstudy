%%configure -f
{ "conf": {"spark.jars.packages": "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1,org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.0.1,org.apache.kafka:kafka-clients:2.6.1,org.elasticsearch:elasticsearch-spark-30_2.12:7.17.3" }}


#import & create spark session
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql.types import *
from pyspark.sql.functions import from_json
from pyspark.sql.functions import col, asc, desc

spark = SparkSession.builder.getOrCreate()

# set es index auto create
spark.conf.set("spark.es.index.auto.create", "true")
spark.conf.set("spark.sql.streaming.schemaInference", "true")


# define schema (json nested type)
schema = StructType([ 
    StructField("@timestamp",StringType(),True), 
    StructField("stb_mac",StringType(),True), 
    StructField("device_model",StringType(),True), 
    StructField("page_type",StringType(),True), 
    StructField("vod_watch_type",StringType(),True), 
    StructField("manufacturer",StringType(),True), 
    StructField("page_id",StringType(),True), 
    StructField("log_type",StringType(),True), 
    StructField("app_release_version",StringType(),True), 
    StructField("action_id",StringType(),True), 
    StructField("client_ip",StringType(),True), 
    StructField("browser_version",StringType(),True), 
    StructField("service_name",StringType(),True), 
    StructField("os_version",StringType(),True), 
    StructField("session_id",StringType(),True), 
    StructField("poc_type",StringType(),True), 
    StructField("url",StringType(),True), 
    StructField("log_time",StringType(),True), 
    StructField("server_received_time",StringType(),True), 
    StructField("browser_name",StringType(),True),
    StructField("web_page_version",StringType(),True),
    StructField("stb_id",StringType(),True),
    StructField("app_build_version",StringType(),True),
    StructField("os_name",StringType(),True),
    StructField("pcid",StringType(),True),
    StructField("client_ip_logserver",StringType(),True),
    StructField("device_base_time",StringType(),True),
    StructField("contents_body", StructType([
        StructField("channel_name", StringType(), True),
        StructField("genre_text", StringType(), True),
        StructField("cid", StringType(), True),
        StructField("purchase", StringType(), True),
        StructField("genre_code", StringType(), True),
        StructField("episode_id", StringType(), True),
        StructField("episode_resolution_id", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("monthly_pay", StringType(), True),
        StructField("list_price", StringType(), True),
        StructField("paid", StringType(), True),
        StructField("type", StringType(), True),
        StructField("title", StringType(), True),
        StructField("purchase_type", StringType(), True),
        StructField("listrunning_time_price", StringType(), True),
        StructField("payment_price", StringType(), True),
       ])),
    StructField("action_body", StructType([
        StructField("position", StringType(), True),
       ])),
    StructField("host", StructType([
        StructField("architecture", StringType(), True),
        StructField("os", StructType([
            StructField("platform", StringType(), True),
            StructField("version", StringType(), True),
            StructField("family", StringType(), True),
            StructField("name", StringType(), True),
            StructField("kernel", StringType(), True),
            StructField("codename", StringType(), True),
           ])),
        StructField("id", StringType(), True),
        StructField("containerized", StringType(), True),
        StructField("name", StringType(), True),
        StructField("hostname", StringType(), True),
       ])),
    StructField("member", StructType([
        StructField("birthyear", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("nickname", StringType(), True),
       ])),       
  ])


  # source - kafka
  # set source kafka
bootstrap_server = 'msk bootstrap server'
topic = 'topic'


# read stream from kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", bootstrap_server) \
    .option("subscribe", topic) \
    .option("startingOffsets", "latest") \
    .load()


# transform (business logic 위치)
# get value from kafka topic
df = df.selectExpr("CAST(value AS STRING)")
dfOut = df.withColumn("value", from_json(df.value, schema)).select("value.*")
dfOut.printSchema()


# es server & index (resource)
# checkpoint 위치는 지정 필요
es_node = "es_ip"
es_port = "9200"
es_resource = "msk/topic"
es_checkpoint_loc = "s3://stream/topic/checkpoint"


# sink to es (30초 간격)
# 강제 종료 시까지 반복 수행
# es에서 data 확인 가능 
output = dfOut \
    .writeStream \
    .format("es")\
    .option("es.nodes", es_node)\
    .option("es.port", es_port)\
    .option("es.resource", es_resource) \
    .option("checkpointLocation", es_checkpoint_loc) \
    .trigger(processingTime = "30 seconds")\
    .start()

output.awaitTermination()




# 참고 - read es data
df = spark.read\
    .format("es")\
    .option("es.nodes", es_node)\
    .option("es.port", es_port)\
    .load(es_resource)

df.groupBy("action_id").count().orderBy("count", ascending=False).show()
df.groupBy("contents_body.channel_name").count().orderBy("count", ascending=False).show()