import socket, json
from tweepy import OAuthHandler, Stream
from tweepy.streaming import StreamListener

consumer_key = ""
consumer_key_secret = ""
access_token = ""
access_token_secret = ""

class TweetListener(StreamListener):

    def __init__(self,csocket):
        self.client_socket = csocket
    
    def on_data(self,data):
        try:
            message = json.loads(data)
            print(message.encode('utf-8'))
            self.client_socket.send(message.encode('utf-8'))
            return True
        except BaseException as e:
            print("Error:",e)
        return True

    def on_error(self,status):
        print(self.client_socket,status)
        return True

def send_data(client_socket):
    auth = OAuthHandler(consumer_key,consumer_key_secret)
    auth.set_access_token(key=access_token,secret=access_token_secret)
    twitter_stream = Stream(auth=auth,listener=TweetListener(client_socket))
    twitter_stream.filter(track=['football'])

if __name__ == '__main__':
    s = socket.socket()
    host = 'localhost'
    port = 9999
    s.bind((host, port))
    print(f"Listening on port {port}")
    s.listen(5)
    c, addr = s.accept() # Establish a connection with the client
    print("Received request from: " + str(addr))
    send_data(c)