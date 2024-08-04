from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, window, count, desc

# Initiate a Spark Session
spark = SparkSession.builder.appName('twitter_spark_streaming').getOrCreate()

# Create a DataFrame from the tweet stream
tweets = spark.readStream.format("socket").option("host", "localhost").option("port", 9000).load()

# Extract hashtags
hashtags_df = tweets.select(explode(col("value")).alias("tweet")) \
                   .select(explode(col("tweet.entities.hashtags")).alias("hashtag")) \
                   .select(col("hashtag.text").alias("hashtag"))

# Calculate hashtag popularity
window_duration = "1 minute"
slide_duration = "10 seconds"
hashtag_counts = hashtags_df.groupBy(window(col("timestamp"), window_duration, slide_duration), col("hashtag")) \
                           .count() \
                           .orderBy(desc("count"))

# Write results to console (for demonstration)
query = hashtag_counts.writeStream.outputMode("complete").format("console").start()

query.awaitTermination()

