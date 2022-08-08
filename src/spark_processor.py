# Apache Spark
from pyspark.sql import SparkSession
from pyspark.sql import functions as fn
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, ArrayType, TimestampType, DoubleType, LongType

# NLTK Sentiment Analyzer
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer

import configparser

parser = configparser.ConfigParser(interpolation=None)
parser.read("credentials.conf")
kafka_topic_name = parser.get("kafka_config", "kafka_topic_name")
kafka_bootstrap_servers = parser.get("kafka_config", "kafka_bootstrap_servers")
psql_url = parser.get("postgres_config", "url")
psql_table = parser.get("postgres_config", "table")
psql_user = parser.get("postgres_config", "user")
psql_password = parser.get("postgres_config", "password")

def polarity(text, analyzer):
    score = analyzer.polarity_scores(text)["compound"]
    return score

def foreachbatchfn(df, epoch_id):
    df.write\
        .format("jdbc")\
        .option("url", psql_url)\
        .option("dbtable",psql_table)\
        .option("user",psql_user)\
        .option("password", psql_password)\
        .option("driver", "org.postgresql.Driver") \
        .mode("append")\
        .save()

# Build Spark Session
spark = SparkSession \
    .builder \
    .appName("Spark streaming with Kafka") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Build Sentiment Intensity Analyzer
sia = SentimentIntensityAnalyzer()


# Spark streaming
df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
  .option("subscribe", kafka_topic_name) \
  .option("startingOffsets", "latest") \
  .load()

schema = StructType()\
    .add("tweet_id", StringType())\
    .add("creation_timestamp", StringType())\
    .add("keyword", StringType())\
    .add("text", StringType())

# SQL User defined function
polarityUDF = fn.udf(lambda x: polarity(x, analyzer=sia), DoubleType())
spark.udf.register("polarityUDF", polarityUDF)

json_df = df.select(
    fn.from_json(fn.col("value").cast("string"),schema).alias("jsondata")
    ).select("jsondata.*")

json_df = json_df.withColumn("tweet_id", json_df["tweet_id"].cast(LongType()))
json_df = json_df.withColumn("creation_timestamp", json_df["creation_timestamp"].cast(TimestampType()))

clean_json_df = json_df\
    .withColumn(
        "clean_text", 
        fn.regexp_replace("text", r"@[\w]+|#|(\w+:\/\/\S+)", "")
        )

polarity_score = clean_json_df\
    .withColumn(
        "polarity_score", polarityUDF(fn.col("clean_text"))
        )

polarity_score = polarity_score.drop("clean_text")
polarity_score.writeStream.foreachBatch(foreachbatchfn).start().awaitTermination()

