from dotenv import load_dotenv
load_dotenv()
import os
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import smart_open

ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
BUCKET_NAME = "cs498mc"
KEY_NAME = "tweets.json"

conf = SparkConf().setMaster("local").setAppName("TotalPositiveTest").set("spark.jars", "{}/jars/*.jar".format(os.path.dirname(__file__))).set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)

# key = boto.connect_s3().get_bucket("cs498mc").get_key("tweets.json")

def main():
    tweets = sqlContext.read.json("s3a://{}:{}@{}/{}".format(ACCESS_KEY, SECRET_KEY, BUCKET_NAME, KEY_NAME))
    tweets.printSchema()

if __name__ == "__main__":
    main()
