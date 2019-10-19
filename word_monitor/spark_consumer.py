import matplotlib
matplotlib.use('Agg')  # Use special backend for Python2; can omit for Python3
import matplotlib.pyplot as plt

from pyspark.streaming import StreamingContext
from pyspark import SparkConf, SparkContext
from pyspark.streaming.kafka import KafkaUtils

def load_word_list(word_list_file):
    words = set()
    f = open(word_list_file, "r")
    lines = f.read().split("\n")
    for line in lines:
        words.add(line)
    f.close()
    return words
    
def construct_plot(counts):
    pwords_counts = []
    nwords_counts = []

    for feeling_fields in counts:
        if feeling_fields:
            # val[0] negative field, val[1] positive field
            nwords_counts.append(feeling_fields[0][1])
            pwords_counts.append(feeling_fields[1][1])

    time = []
    for i in range(len(pwords_counts)):
        time.append(i)

    pos_line = plt.plot(time, pwords_counts, 'ro-', label='pfeelings words')
    neg_line = plt.plot(time, nwords_counts, 'ko-', label='nfeelings words')
    plt.axis([0, len(pwords_counts) - 1, 0, max(max(pwords_counts), max(nwords_counts))+40])
    plt.xlabel('time')
    plt.ylabel('count')
    plt.legend(loc = 'upper right')
    plt.savefig('feelingAnalysis.png')

def main():
    # load positive/negative word list
    # Use absolute file paths if necessary
    nfeeling_words = load_word_list("../word_monitor/dataset/nFeeling.txt")
    pfeeling_words = load_word_list("../word_monitor/dataset/pFeeling.txt")
        
    # Initialize spark streaming context
    conf = SparkConf().setAppName("TwitterStreamApplication")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 1)
    ssc.checkpoint("checkpoint_TwitterStreamApplication")

    # Processing data from Kafka
    kstream = KafkaUtils.createDirectStream(ssc, ["twitter-stream"], {"metadata.broker.list": "localhost:9092"})
    tweets = kstream.map(lambda x: x[1].encode("ascii", "ignore"))  # Extract and only keep ascii-supported messages
    words = tweets.flatMap(lambda line: line.split(" "))  # Extract all words in tweets and put in a list
    nfeelings = words.map(lambda word: ("nfeelings", 1) if word in nfeeling_words else ("nfeelings", 0))
    pfeelings = words.map(lambda word: ("pfeelings", 1) if word in pfeeling_words else ("pfeelings", 0))
    both_feelings = pfeelings.union(nfeelings)
    feeling_counts = both_feelings.reduceByKey(lambda x,y: x+y)  # Reduce by key (nfeelings or pfeelings) -> [(nfeelings, count), (pfeelings, count)]
    
    counts = []
    feeling_counts.foreachRDD(lambda t,rdd: counts.append(rdd.collect()))  # foreachRDD() takes a function that has two parameter: (time, rdd)

    ssc.start()
    ssc.awaitTerminationOrTimeout(10)  # Set running time
    ssc.stop(stopGraceFully = True)

    construct_plot(counts)

if __name__=="__main__":
    main()
