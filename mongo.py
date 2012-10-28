import pymongo

class Mongo():

	def connect(self):
		
		conn = pymongo.Connection("mongodb://localhost/", 27017)
		
		return {'conn':conn, 'db':conn.yourDBName}
		