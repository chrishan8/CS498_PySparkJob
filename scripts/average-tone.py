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

spark = SparkSession.builder.master("local[*]").appName("TotalPositiveTest").config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem").config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY).config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY).getOrCreate()

def main():
    # tweetsDF = spark.read.json("s3a://{}/{}".format(BUCKET_NAME, KEY_NAME))
    tweetsDF = spark.read.json(os.path.join(os.path.dirname(__file__), '../data/cleaned.json'))

    tweetsDF.createOrReplaceTempView("tweets")
    toneDF = spark.sql("SELECT entities.hashtags.text, tone.document_tone.tones FROM tweets")
    toneDFExploded = toneDF.withColumn("text", explode(toneDF.text)).withColumn("tones", explode(toneDF.tones))
    tonesByHashtag = toneDFExploded.rdd.filter(lambda x: x.text in HASHTAGS).map(lambda x: ((x.text, x.tones.tone_name), (x.tones.score, 1))).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])).map(lambda x: ((x[0][0], x[0][1]), x[1][0] / x[1][1])).sortByKey()

    currentHashtag = None
    for ((hashtag, tone), average_score) in tonesByHashtag.collect():
        if currentHashtag != hashtag:
            currentHashtag = hashtag
            print "Hashtag: %s\n\tTone: %s, Average Score: %s" % (hashtag, tone, average_score)
        else:
            print "\tTone: %s, Average Score: %s" % (tone, average_score)

if __name__ == "__main__":
    main()
