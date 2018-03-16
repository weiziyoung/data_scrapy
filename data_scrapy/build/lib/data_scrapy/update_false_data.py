from peewee import *
from base_model import Job
mysql_db = MySQLDatabase(host='127.0.0.1', user='root', passwd='young1001', database='archive_job',
                                      charset='utf8')
mysql_db.connect()
