import pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("PopularTweets")


## Load tweets from HDFS
tweetsDF = spark.read.json(os.path.join(os.path.dirname(__file__), '../data/cleaned.json'))
tweetsDF.createOrReplaceTempView("tweets")
hashTags= spark.sql("SELECT hashtags from tweets")
hashTags_rev = hashTags.map(lambda x: (x.split()[1]), 1)
hashTagsCounts= hashTags_rev.reduceByKey(lambda x, y: x+y)
flipped = hashTagsCounts.map(lambda xy: (xy[1], xy[0]))

sortedHashTags = flipped.sortByKey()
for result in results:
    print(result)
