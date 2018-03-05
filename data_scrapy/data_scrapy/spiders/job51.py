from data_scrapy.items import JobItem
from data_scrapy.base_spider import BaseSpider
from scrapy import Request
from scrapy.selector import Selector
from scrapy.exceptions import CloseSpider
import pickle
import logging


class Job51Spider(BaseSpider):
    name = 'job51'
    allowed_domains = ['search.51job.com', 'jobs.51job.com']

    def start_requests(self):
        start_urls = []
        with open('51_city_code.pkl', 'rb') as f:
            city_codes = pickle.load(f)
        code_list = []
        for city in self.city_list:
            temp = city_codes.get(city, '')
            if temp:
                code_list.append(temp)
        for code in code_list:
            for word in self.INSPECT_WORDS:
                start_urls.append('http://search.51job.com/jobsearch/search_result.php?'
                                  'jobarea={}&keyword={}'.format(code, word))
        for url in start_urls:
            yield Request(url=url, callback=self.parse)

    def parse(self, response):
        sel = Selector(response)
        items = sel.css('div .dw_table div.el')[1:]
        count = 3
        for item in items:
            job_name = item.css('p.t1>span>a::text').extract()[0].strip()
            job_company_name = item.css('span.t2>a::text').extract()[0]
            flag = False
            for word in self.FILTER_WORDS:
                if word in job_name or word in job_company_name:
                    url = item.css('p.t1>span>a').xpath('@href').extract()[0]
                    count = 3
                    flag = True
                    yield Request(url=url, callback=self.parse_item)
                    break
            if not flag:
                logging.log(logging.INFO, "There is no key word in the position's name--{}".format(job_name))
                count -= 1
                if not count:
                    logging.log(logging.INFO, "No key words for three times,Stop!")
                return
        next_page = sel.css('li.bk>a')
        if next_page:
            if len(next_page) == 1:
                link = next_page.xpath('@href').extract()[0]
            else:
                link = next_page.xpath('@href').extract()[1]
            yield Request(url=link, callback=self.parse)

    def parse_item(self, response):
        if response.status != 200:
            raise CloseSpider(reason='This spider has been blocked')
        sel = Selector(response)
        item = JobItem()
        item['url'] = response.url
        item['from_website'] = self.name
        head_info = sel.css('div.cn')
        job_name = head_info.css('h1::text').extract()[0]
        item['name'] = job_name
        location = head_info.css('span.lname::text').extract()[0]
        item['location'] = location.split('-')[0]
        item['company_name'] = head_info.css('p.cname>a::text').extract()[0]
        temp = [each.strip() for each in head_info.css('p.msg::text').extract()[0].split('|')]
        if len(temp) == 3:
            item['company_type'], item['company_scale'], item['company_industry'] = temp
        elif len(temp) == 2:
            if '公司' in temp[0]:
                item['company_type'], item['company_industry'] = temp
                item['company_scale'] = 'Unknown'
            else:
                item['company_scale'], item['company_industry'] = temp
                item['company_type'] = 'Unknown'
        elif len(temp) == 1:
            item['company_type'] = temp[0]
            item['company_scale'] = item['company_industry'] = 'Unknown'
        else:
            raise CloseSpider('A bug was found,url is %s' % response.url)
        try:
            item['salary'] = head_info.css('div.cn strong::text').extract()[0]
        except IndexError:
            item['salary'] = '面议'
        middle_info = sel.css('div.t1')
        requirements = middle_info.css('span.sp4::text').extract()
        assert len(requirements) == 4 or len(requirements) == 3
        if len(requirements) == 4:
            item['exp'], item['edu'], item['available_number'], item['pub_date'] = requirements
        elif len(requirements) == 3:
            item['exp'], item['available_number'], item['pub_date'] = requirements
            item['edu'] = 'Unknown'
        text = ''.join([each.strip()for each in sel.css('div .job_msg::text').extract()])
        attribute = ['duty', 'demand', 'welfare', 'work_time', 'address']
        item['duty'] = item['demand'] = item['welfare'] = item['work_time'] = item['address'] = ''
        flags = []
        # Point out positions of above key words
        for words in self.KEY_WORDS:
            for word in words:
                flag = text.find(word)
                if flag != -1:
                    flags.append(flag)
                    break
        flags.sort()
        # Then, process these positions one by one
        for flag in range(len(flags)):
            quit_not = False
            # cut the string into various sections
            if flag == len(flags) - 1:
                message = text[flags[flag]:]
            else:
                message = text[flags[flag]:flags[flag + 1]]
            # For each section, we will find the key words of it.
            for n, words in enumerate(self.KEY_WORDS):
                for word in words:
                    if word in message:
                        key = attribute[n]
                        # cut off some unnecessary words
                        word_length = len(word)
                        item[key] = message[word_length:]
                        quit_not = True
                        break
                if quit_not:
                    break
        address = ''.join(sel.css('div.bmsg>p.fp::text').extract()).strip()
        if location not in self.city_list:
            for city in self.city_list:
                if city in address:
                    item['location'] = city
                    break
            else:
                logging.log(logging.WARNING, 'The city -- %s is not in the list,url:%s' % (location, response.url))
        item['address'] = address
        item['job_category'] = 'Unknown'
        yield item
