from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import sys

# Variables that contains the user credentials to access Twitter API
CONSUMER_KEY = 'VScoLsA2Ip84Gg2GpR2Imf5Fy'
CONSUMER_SECRET = '0KUiTPhNbgb6koaspDVOHlh2e955ZPqXVNhqrLN9wdWsL4cUV3'
access_token = "1868826828-teIYVXW5m5uS4HBTDkE7vfqa87BqlKkmaeo0X4c"
access_token_secret = "1pEZ3bSj4Tbg8Xag7kvOLrXjryijbT33kfZfNtu6Bq6do"

# This is a basic listener that just prints received tweets to stdout.
class StdOutListener(StreamListener):
    def on_data(self, data):
        print(data)
        return True

    def on_error(self, status):
        print("error")
        print(status)

if __name__ == '__main__':

    q = 'hurricane_twitter.json'
    f = open(q, 'w')
    sys.stdout = f

    # This handles Twitter authentication and the connection to Twitter Streaming API
    l = StdOutListener()
    auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, l)

    stream.filter(track=['hurricane', 'Hurricane Irma', 'Florida', 'hurricane harvey', 'houston', 'corps christi',
                         'hurricane irma', 'irma', 'hurricaneirma', 'Irmahurricane', 'hurricaneirma2017', 'hurricaneharvey', 'HoustonFloods',
                         'HurricaneHarveyRelief', 'HurricaneIrmaRelief', 'Barbuda', 'St Barts', 'St Martin', 'Miami',
                          'Florida Keys island', 'Turks', 'Caicos', 'Haiti', 'Puerto Rico'])



