import io.archivesunleashed.spark.matchbox._
import io.archivesunleashed.spark.matchbox.TweetUtils._
import io.archivesunleashed.spark.rdd.RecordRDD._

## Load tweets from HDFS
tweets = ftweetsDF = spark.read.json(os.path.join(os.path.dirname(__file__)

## Count them
tweets.count()

## Extract some fields
val r = tweets.map(tweet => (tweet.id, tweet.createdAt, tweet.username, tweet.text, tweet.lang,
                             tweet.isVerifiedUser, tweet.followerCount, tweet.friendCount))

##  Take a sample of 10 on console
r.take(10)

## Count the number of hashtags

val hashtags = tweets.map(tweet => tweet.text)
                     .filter(text => text != null)
                     .flatMap(text => {"""#[^ ]+""".r.findAllIn(text).toList})
                     .countItems()

hashtags.take(10)

