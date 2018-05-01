from dotenv import load_dotenv
load_dotenv()
import os
from pyspark import SparkConf
from pyspark.sql import SQLContext, SparkSession

ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
BUCKET_NAME = "cs498mc"
KEY_NAME = "cleaned.json"

spark = SparkSession.builder.master("local[*]").appName("TotalPositiveTest").config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem").config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY).config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY).getOrCreate()

def main():
    tweetsDF = spark.read.json("s3a://{}/{}".format(BUCKET_NAME, KEY_NAME))
    # tweetsDF = spark.read.json(os.path.join(os.path.dirname(__file__), '../data/cleaned.json'))
    tweetsDF.createOrReplaceTempView("tweets")
    sentimentDF = spark.sql("SELECT tone.sentiment FROM tweets")
    totalSentiments = sentimentDF.rdd.map(lambda x: (x.sentiment, 1)).reduceByKey(lambda x, y: x + y)
    for result in totalSentiments.collect():
        print "Sentiment: %s, Total: %s" % (result[0], result[1])

if __name__ == "__main__":
    main()
