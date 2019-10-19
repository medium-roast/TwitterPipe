from tweepy import OAuthHandler, Stream, StreamListener
from kafka import SimpleProducer, KafkaClient

# Replace the values below with yours Access Token
consumer_key="YOUR_CONSUMER_KEY"
consumer_secret="YOUR_CONSUMER_SECRET"
access_token="YOUR_ACCESS_TOKEN"
access_token_secret="YOUR_ACCESS_TOKEN_SECRET"

class KafkaListener(StreamListener):
    """ A listener handles tweets that are received from the stream.
    This is a basic listener that just prints received tweets to stdout.
    """
    def on_data(self, data):
	    # Send message to Kafka from the producer, "twitter-stream" is the topic
        producer.send_messages("twitter-stream", data.encode("utf-8"))  
        return True

    def on_error(self, status):
        print(status)

if __name__ == '__main__':
	# Create a simple producer as Kafka client
    kafka_client = KafkaClient("localhost:9092")  
    producer = SimpleProducer(kafka_client)

    l = KafkaListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

	# Create a stream
    stream = Stream(auth, l)
    stream.filter(track=['#'])  # parameters: e.g. language='en', track=[search keywords for tweets]
