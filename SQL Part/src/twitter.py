import constants
import oauth2
import urllib.parse as urlparse
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import json
import tweepy

#Variables that contains the user credentials to access Twitter API
CONSUMER_KEY = 'VScoLsA2Ip84Gg2GpR2Imf5Fy'
CONSUMER_SECRET = '0KUiTPhNbgb6koaspDVOHlh2e955ZPqXVNhqrLN9wdWsL4cUV3'
AUTHORIZATION_URL = 'https://api.twitter.com/oauth/authorize'
REQUEST_TOKEN_URL = 'https://api.twitter.com/oauth/request_token'
ACCESS_TOKEN_URL = 'https://api.twitter.com/oauth/access_token'

#these allow twitter to identify this app
# Create a consumer, which uses CONSUMER_KEY and CONSUMER_SECRET to identify our app uniquely
consumer = oauth2.Consumer(CONSUMER_KEY, CONSUMER_SECRET)

#this is used to make request
client = oauth2.Client(consumer)

# Use the client to perform a request for the request token
response, content = client.request(REQUEST_TOKEN_URL, 'POST')
if response.status !=200:
    print("an error occurred getting the request token from Twitter!")

#Get the request token parsig the query string returnerd
request_token = dict(urlparse.parse_qsl(content.decode('utf-8')))


#www.ourwebsite.com "log in with twitter button"
#they press "Sign in" or "authorize"
#Twitter sends them back to e.g www.ourwebsite.com/auth
#We get that auth code + request token -> twitter -> access token

# Ask the user to authorize our app and give us the pin code
print("Go to the following site in yor browser:")
print("{}?oauth_token={}".format(AUTHORIZATION_URL, request_token['oauth_token']))

oauth_verifier = input("What is the PIN? ")

#Create a Token object wich contains the request token, and the verifier
token = oauth2.Token(request_token['oauth_token'], request_token['oauth_token_secret'])
token.set_verifier(oauth_verifier)

#Create a clinet with our consuner (our app) and the newly created (and verfied) token
client = oauth2.Client(consumer, token)

#Ask Twitter for an access token, and Twitter Knows it should give us it because we've verified the request token
response, content = client.request(ACCESS_TOKEN_URL, 'POST')
access_token = dict(urlparse.parse_qsl(content.decode('utf-8')))


#Create an 'authorized_token' Token object and use that to perform Twitter API calls on behalf of the user
authorized_token = oauth2.Token(access_token['oauth_token'], access_token['oauth_token_secret'])
authorized_client = oauth2.Client(consumer, authorized_token)

#Make Twitter API calls!
response, content = authorized_client.request('https://api.twitter.com/1.1/search/tweets.json?q=Manhattan,KS')
if response.status != 200:
    print("An error occurred when searching!")

print(content.decode('utf-8'))

tweets = json.loads(content.decode('utf-8'))

for tweet in tweets['statuses']:
    print(tweet['text'])