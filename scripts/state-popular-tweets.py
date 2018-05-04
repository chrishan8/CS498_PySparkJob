
import io.archivesunleashed.spark.matchbox._
import io.archivesunleashed.spark.matchbox.TweetUtils._
import io.archivesunleashed.spark.rdd.RecordRDD._

tweets = spark.read.json(os.path.join(os.path.dirname(__file__)
df = (
    tweets
    .withColumn("tokens", tokenize_udf(input_df['body']))
    .withColumn("state", extract_state(input_df['gnip.profileLocations']))
    .filter('state IS NOT NULL')
    .filter(is_from_official_client(input_df['generator.displayName']))
    .withColumn("user_id", remove_user_id_prefix(input_df['actor.id']))
)
df_tokens = df.select(df.id, df.user_id, F.explode(df.tokens).alias('word'))
df_tokens.cache()

df_topics_by_state = (
    df_tokens
    .join(df_topic_words, on='word')
    .join(df_user_states, on='user_id')
    .select('user_id', 'topic', 'state')
    .groupby(['state', 'topic'])
    .agg(F.countDistinct('user_id').alias('n_users'))
)

df_topics_by_state.cache()
