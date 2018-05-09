
from pyspark import SparkConf, SparkContext

conf= SparkConf().setMaster("local").setAppName("PopularTweetsByStates")
tweetsDF = spark.read.json(os.path.join(os.path.dirname(__file__), '../data/cleaned.json'))
tweetsDF.createOrReplaceTempView("tweets")
hashTags_and_state= spark.sql("SELECT hashtags, state from tweets group by state")
hashtags = hashTags_and_state.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

results = hashtags.collect()
for result in results:
    print(result)
