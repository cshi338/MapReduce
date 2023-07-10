# First draft of finding the word length using Spark. Need to update this program with python unit testing.
# Also need to compare the word length results between Spark and MapReduce to check for accuracy.

import pyspark

if __name__=="__main__":
    sc = pyspark.SparkContext()

    words = sc.textFile("100west.txt")
    counts = words.flatMap(lambda line: line.split()).map(lambda word: (len(word), 1)).reduceByKey(lambda x, y: x + y)
    word_counts = counts.collect()
    word_counts.saveAsTextFile("wordLength_sparkout.txt")