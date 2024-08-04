# DStream is deprecated as of Spark 3.4.0. Migrate to Structured Streaming.
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc = SparkContext("local[2]","NetworkWordCounter")
ssc = StreamingContext(sc,1)

lines = ssc.socketTextStream(hostname='localhost',port=8000)

words = lines.flatMap(lambda line: line.split(' '))

pairs = words.map(lambda word: (word,1))

word_counts = pairs.reduceByKey(lambda num1, num2: num1+num2)

word_counts.pprint()

ssc.start()
