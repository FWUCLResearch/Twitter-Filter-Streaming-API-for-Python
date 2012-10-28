import pymongo, requests, json, threading, time
from mongo import Mongo
from ConsumeTweetStream import ConsumeTweetStream

class Miner():

	def __init__(self, toTrack):
		
		self.mongo = Mongo().connect()
		
		self.connections = []
		
		self.toTrack = toTrack
		
		self.auth = ('user', 'passwd')
		
		self.connection_changed = 0
		
	#
	#	Name: MineTweetsFromStreaming
	#	Desc: Open a Connection to the Streaming Filter API + track 
	#		  users in the db or keywords or both
	
	def MineTweetsFromStreaming(self):
		
		self.connections.append(requests.post("https://stream.twitter.com/1/statuses/filter.json", data={'track':self.toTrack}, auth=self.auth))
		
		UpdatePredictatesThread = threading.Thread(target=self.UpdatePredictates)
		
		ConsumeTweetStreamThreads = []
		
		ConsumeTweetStreamThreads.append(ConsumeTweetStream(self.connections, self.mongo))
		
		UpdatePredictatesThread.start()
		
		ConsumeTweetStreamThreads[0].start()
		
		while 1:
			
			if self.connection_changed == 1:
				
				ConsumeTweetStreamThreads[0].join()
				ConsumeTweetStreamThreads.pop(0)
				ConsumeTweetStreamThreads.append(ConsumeTweetStream(self.connections, self.mongo))
				ConsumeTweetStreamThreads[0].start()
				self.connection_changed = 0

				
	#
	#	Name: MineTweetsFromREST
	#   Desc: Get Tweets that may have been missed during the period 
	#		  of when the user signed up to when the user's a/c was being tracked in the main stream.
					
	def MineTweetsFromREST(self, toTrack):
	
		r = requests.get("https://api.twitter.com/1/statuses/user_timeline.json?include_entities=true&include_rts=true&screen_name="+toTrack+"&count=200", verify=False)
		
		for tweet in json.loads(r.content):
			
			self.mongo['db'].tweets.insert(tweet)
			
	#
	#	Name: UpdatePredictates
	#	Desc: Swapes old connection out for new one with new users to track.
	
	def UpdatePredictates(self):
	
		while 1:
			new_users_queue = self.mongo['db'].new_users_queue.find()
		
			if new_users_queue.count() >= 12:
				
				for new_user in new_users_queue:
					
					self.toTrack.append(new_user['screen_name'])
					
				new_connection = requests.post("https://stream.twitter.com/1/statuses/filter.json", data={'track':self.toTrack}, auth=self.auth)
				
				if new_connection.status_code is 200:
						print self.toTrack
						self.connections.pop(0)
						self.connections.append(new_connection)
						self.connection_changed = 1
						self.mongo['db'].new_users_queue.remove({})
						print 'appended new connection to connections array!'
				else:
					
					attempts = 1
					
					while new_connection.status_code is not 200:
						
						self.Backoff(attempts)
						
						print 'Backed off, tried to connect '+str(attempts)+' times.'
						
						new_connection = requests.post("https://stream.twitter.com/1/statuses/filter.json", data={'track':self.toTrack}, auth=self.auth)
						
						if new_connection.status_code is 200:
							break
						else:
							attempts += 1
						
						
			time.sleep(30)
	
	
	#
	#	Name: Backoff
	#	Desc: Backoff the API when we're getting the finger from Twitter.
	
	def Backoff(self, iterations):
	
		if iterations < 2:
			time.sleep(2)
		elif iterations < 3:
			time.sleep(4)
		elif iterations < 4:
			time.sleep(8)
		elif iterations < 5:
			time.sleep(12)
		elif iterations < 6:
			time.sleep(16)
		elif iterations < 7:
			time.sleep(60*5)
		elif iterations < 8:
			time.sleep(60*10)
		else:
			pass
		
	
					
Miner(['Dell','michael','Apple']).MineTweetsFromStreaming()
#Miner().MineTweetsFromREST('james_eggers')