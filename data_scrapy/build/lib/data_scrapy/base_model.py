from peewee import *
from datetime import datetime

mysql_db = MySQLDatabase(host='127.0.0.1', user='root', passwd='young1001', database='archive_job', charset='utf8')


class Job(Model):
    url = CharField(primary_key=True, max_length=255)
    name = CharField(max_length=255)
    location = CharField(max_length=10)
    salary = CharField(max_length=20)
    pub_date = DateField()
    available_number = CharField(max_length=20, null=True)
    job_category = CharField(max_length=20, null=True)
    exp = CharField(max_length=10)
    edu = CharField(max_length=10)
    duty = TextField()
    demand = TextField()
    welfare = TextField()
    work_time = TextField()
    address = TextField()
    company_name = CharField(max_length=255)
    company_scale = CharField(max_length=20)
    company_type = CharField(max_length=20, null=True)
    company_industry = CharField(max_length=50, null=True)
    from_website = CharField(max_length=10)
    scrape_time = DateTimeField()

    class Meta:
        database = mysql_db

if __name__ == '__main__':
    mysql_db.connect()
    try:
        mysql_db.create_tables([Job])
    except OperationalError as e:
        print('already has a table')
        pass