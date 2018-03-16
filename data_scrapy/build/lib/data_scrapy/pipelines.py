import logging
import re
from datetime import timedelta, date, datetime
from peewee import *
from scrapy.exceptions import DropItem
from data_scrapy.base_model import Job


class ZhilianPipeLine(object):
    def process_item(self, item, spider):
        def process_salary(salary):
            temp = re.findall(r'\d+', salary)
            if len(temp) == 2:
                return str((int(temp[0]) + int(temp[1])) / 2)
            if len(temp) == 1:
                return str(int(temp[0]))
            if not temp:
                return salary.strip()

        def process_date(pub_date):
            if re.match(r'\d{4}-\d{2}-\d{2}', pub_date):
                p_date = re.match(r'\d{4}-\d{2}-\d{2}', pub_date).group()
                if p_date.startswith('0002'):
                    raise DropItem('Out of Date')
                else:
                    return p_date
            else:
                today = date.today()
                if pub_date == '今天' or pub_date == '刚刚' or pub_date == '小时前' or pub_date == '最新':
                    result = today
                elif pub_date == '昨天':
                    delta = timedelta(days=-1)
                    result = today + delta
                elif pub_date == '前天':
                    delta = timedelta(days=-2)
                    result = today + delta
                elif '天前' in pub_date:
                    num = int(re.search(r'\d+', pub_date).group())
                    delta = timedelta(days=-num)
                    result = today + delta
                else:
                    raise DropItem("不明日期表示")
                return result
        if item['from_website'] != 'zhilian':
            return item
        item['pub_date'] = process_date(item['pub_date'])
        item['salary'] = process_salary(item['salary'])
        return item


class BossPipeLine(object):
    def process_item(self, item, spider):
        if item['from_website'] != 'boss':
            return item

        def process_date(pub_date):
            pub_date = pub_date.replace('发布于', '')
            if ':' in pub_date:
                return date.today()
            if pub_date == '昨天':
                return date.today()+timedelta(days=-1)
            else:
                temp = re.search(r'(\d{2})月(\d{2})日', pub_date)
                if temp:
                    month, day = temp.group(1), temp.group(2)
                    _today = date.today()
                    if int(month) > _today.month:
                        year = _today.year - 1
                        return str(year) + '-' + month + '-' + day
                    elif int(month) == _today.month:
                        if int(day) <= _today.day:
                            return str(_today.year) + '-' + month + '-' + day
                        else:
                            return str(_today.year-1) + '-' + month + '-' + day
                    else:
                        return str(_today.year) + '-' + month + '-' + day
                else:
                    assert False

        def process_salary(salary):
            temp = re.findall(r'\d+', salary)
            if len(temp) == 2:
                return str((int(temp[0]) + int(temp[1])) / 2 * 1000)
            if len(temp) == 1:
                return str(int(temp[0]))
            if not temp:
                return salary.strip()

        item['pub_date'] = process_date(item['pub_date'])
        item['salary'] = process_salary(item['salary'])
        return item


class Job51PipeLine(object):
    def process_item(self, item, spider):
        if item['from_website'] != 'job51':
            return item

        def process_date(pub_date):
            pub_date = pub_date.replace('发布', '')
            if ':' in pub_date:
                return date.today()
            if pub_date == '昨天':
                return date.today()+timedelta(days=-1)
            else:
                temp = re.search(r'(\d{2})-(\d{2})', pub_date)
                if temp:
                    month, day = temp.group(1), temp.group(2)
                    _today = date.today()
                    if int(month) > _today.month:
                        year = _today.year - 1
                        return str(year) + '-' + month + '-' + day
                    elif int(month) == _today.month:
                        if int(day) <= _today.day:
                            return str(_today.year) + '-' + month + '-' + day
                        else:
                            return str(_today.year-1) + '-' + month + '-' + day
                    else:
                        return str(_today.year) + '-' + month + '-' + day
                else:
                    assert False

        def process_salary(salary):
            re_result = re.search('(\d+\.?\d*)-(\d+\.?\d*)', salary)
            if not re_result:
                re_result = re.search('\d+\.?\d*', salary)
                if re_result:
                    number = float(re_result.group())
                else:
                    return salary.strip()
            else:
                left, right = re_result.group(1), re_result.group(2)
                number = (float(left) + float(right))/2
            if '千' in salary:
                salary_num = number * 1000
            elif '万' in salary:
                salary_num = number * 10000
            elif '元' in salary:
                salary_num = number
            else:
                return salary
            if '月' in salary:
                return str(int(salary_num))
            elif '年' in salary:
                return str(int(salary_num/12))
            elif '天' in salary or '日' in salary:
                return str(int(salary_num*22))
            elif '小时' in salary:
                return str(int(salary_num*8*22))
            else:
                return str(int(salary_num))

        item['pub_date'] = process_date(item['pub_date'])
        item['salary'] = process_salary(item['salary'])
        return item


class LiePinPipeLine(object):
    def process_item(self, item, spider):
        if item['from_website'] != 'liepin':
            return item

        def process_date(pub_date):
            today = date.today()
            result = ''
            if pub_date == '刚刚':
                result = today
            elif pub_date == '昨天':
                result = today + timedelta(days=-1)
            elif pub_date == '前天':
                result = today + timedelta(days=-2)
            elif '小时' in pub_date or '分钟' in pub_date:
                result = today
            elif '月' in pub_date:
                result = today + timedelta(days=-30)
            elif re.match(r'^\d{4}-\d{2}-\d{2}$', pub_date):
                return pub_date
            return result.strftime('%Y-%m-%d')

        def process_salary(salary):
            if salary == '面议':
                return salary
            else:
                re_result = re.search('(\d+)-(\d+)万', salary)
                left, right = re_result.group(1), re_result.group(2)
                aver = int((int(left) + int(right))/2 * 10000 / 12)
                return str(aver)

        def process_edu(edu):
            """
            The input value might from {'中专/中技及以上', '大专及以上', '学历不限', '本科及以上', '硕士及以上', '统招本科'}
            :param edu: string
            :return:
            """
            if '中专' in edu:
                return '中专'
            elif '大专' in edu:
                return '大专'
            elif '不限' in edu:
                return '不限'
            elif '本科' in edu:
                return '本科'
            elif '硕士' in edu:
                return '硕士'
            else:
                logging.log(logging.WARNING, 'The "edu" value is not normal')
        item['pub_date'] = process_date(item['pub_date'])
        item['salary'] = process_salary(item['salary'])
        item['edu'] = process_edu(item['edu'])
        return item


class DataStorePipeLine(object):
    def __init__(self):
        self.mysql_db = MySQLDatabase(host='127.0.0.1', user='root', passwd='young1001', database='archive_job',
                                      charset='utf8')
        self.cache_num = 0
        self.cache_list = []

    def open_spider(self, spider):
        self.mysql_db.connect()
        self.cache_num = spider.settings.get('CACHE_NUMBER')
        self.cache_list = []

    def process_item(self, item, spider):
        item['scrape_time'] = datetime.now()
        info_dict = dict(item)
        with self.mysql_db.atomic():
            try:
                # Insert data
                Job.create(**info_dict)
            # If the table haven't created, the system create it by itself, and then insert it.
            except ProgrammingError:
                logging.log(logging.INFO, '发现未创建数据表,自动创建')
                self.mysql_db.create_tables([Job])
                Job.create(**info_dict)
            # If there is a duplicated data, then drop it!
            except IntegrityError:
                logging.log(logging.INFO, '%s发生重复' % info_dict['url'])
                raise DropItem()
        return item

    def close_spider(self, spider):
        self.mysql_db.close()
