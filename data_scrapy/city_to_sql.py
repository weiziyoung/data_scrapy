from peewee import *
import pickle

mysql_db = MySQLDatabase(host='127.0.0.1',user='root',passwd='young1001',database='archive_job', charset='utf8')

class City(Model):
	city = CharField(primary_key=True,max_length=10)
	provience = CharField(max_length=10)
	
	class Meta:
		database = mysql_db

mysql_db.connect()
f = open('city.pickle','rb')
city_map = pickle.load(f)
mysql_db.create_tables([City])
for i in city_map:
	provience = i[0]
	city = i[1][:-1]
	City.create(city=city,provience=provience)
mysql_db.close()
f.close()
