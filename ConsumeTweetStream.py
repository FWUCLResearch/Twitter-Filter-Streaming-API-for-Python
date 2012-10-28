import pymongo, requests, json, threading, time
from mongo import Mongo

class ConsumeTweetStream(threading.Thread):

		
	def __init__(self, connections, mongo):

		self._stopevent = threading.Event()	
		self._sleepperiod = 0	
		self.connections = connections
		self.mongo = mongo
		
		threading.Thread.__init__(self, name="ConsumeTweetStreams")
		
	def run(self):
		"""
		overload of threading.thread.run()
		main control loop
		"""

		while not self._stopevent.isSet():
			try:			
				for line in self.connections[0].iter_lines():
	
					if line:
					
						print json.loads(line)['text']
						self.mongo['db'].tweets.insert(json.loads(line))
			except KeyError or RuntimeError:
				pass

			self._stopevent.wait(self._sleepperiod)
		

	def join(self,timeout=None):
		"""
		Stop the thread
		"""
		self._stopevent.set()
		threading.Thread.join(self, timeout)
		print 'Thread Shutdown!'

