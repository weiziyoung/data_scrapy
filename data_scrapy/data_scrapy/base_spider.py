from scrapy.spiders import Spider
import pickle
from datetime import date


class BaseSpider(Spider):
    name = ''
    KEY_WORDS = [['岗位职责', '岗位描述', '工作内容', '职位描述'],
                 ['任职要求', '任职条件', '任职资格', '岗位要求', '资质要求'],
                 ['福利待遇', '薪资待遇', '薪酬福利', '薪资福利'],
                 ['工作时间'],
                 ['工作地址', '工作地点', '上班地址']]
    INSPECT_WORDS = ['档案', '文控', '文件管理', '图书管理']
    FILTER_WORDS = ['档案', '文件', '文控', '图书', '资料']
    city_list = [each[1][:-1] for each in pickle.load(open('city.pickle', 'rb'))]

    def start_requests(self):
        pass

    def parse(self, response):
        pass

    def parse_item(self, response):
        pass
