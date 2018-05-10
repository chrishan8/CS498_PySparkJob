import csv
from dotenv import load_dotenv
load_dotenv()
import os
from pyspark import SparkConf
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.functions import explode

ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
BUCKET_NAME = "cs498mc"
KEY_NAME = "cleaned.json"
HASHTAGS = set(['travelban','daca','thewall','guncontrol','shooting','gunsense','nra','healthcare','obamacare','medicare','medicaid'])

spark = SparkSession.builder.master("local[*]").appName("TotalSentiment").config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem").config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY).config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY).getOrCreate()

def main():
    # tweetsDF = spark.read.json("s3a://{}/{}".format(BUCKET_NAME, KEY_NAME))
    tweetsDF = spark.read.json(os.path.join(os.path.dirname(__file__), '../data/cleaned.json'))

    tweetsDF.createOrReplaceTempView("tweets")
    sentimentDF = spark.sql("SELECT entities.hashtags.text, tone.sentiment FROM tweets")

    sentimentDFExploded = sentimentDF.withColumn("text", explode(sentimentDF.text))
    totalSentimentsByHashtag = sentimentDFExploded.rdd.filter(lambda x: x.text in HASHTAGS).map(lambda x: ((x.text, x.sentiment), 1)).reduceByKey(lambda x, y: x + y).sortByKey()

    # currentHashtag = None
    # for ((hashtag, sentiment), count) in totalSentimentsByHashtag.collect():
    #     if currentHashtag != hashtag:
    #         currentHashtag = hashtag
    #         print "Hashtag: %s\n\tSentiment: %s, Total: %s" % (hashtag, sentiment, count)
    #     else:
    #         print "\tSentiment: %s, Total: %s" % (sentiment, count)

    with open(os.path.join(os.path.dirname(__file__), '../output/total-sentiment.csv'), 'wb') as f:
        writer = csv.writer(f, delimiter=',')
        for ((hashtag, sentiment), count) in totalSentimentsByHashtag.collect():
            writer.writerow([hashtag, sentiment, count])

if __name__ == "__main__":
    main()
