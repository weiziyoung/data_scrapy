from data_scrapy.items import JobItem
from data_scrapy.base_spider import BaseSpider
from scrapy import Request
from scrapy.selector import Selector
import logging


class LiePinSpider(BaseSpider):
    name = 'liepin'
    allowed_domains = ['liepin.com']

    def start_requests(self):
        for word in self.INSPECT_WORDS:
            url = 'https://www.liepin.com/zhaopin/?&key={}'.format(word)
            yield Request(url=url, callback=self.parse)

    def parse(self, response):
        sel = Selector(response)
        lis = sel.css('ul.sojob-list li')
        exist = False
        for li in lis:
            job_name = li.css('div.job-info>h3>a::text').extract()[0]
            company_name = li.css('p.company-name>a::text').extract()[0]
            for word in self.FILTER_WORDS:
                if word in job_name or word in company_name:
                    href = li.css('div.job-info h3 a').xpath('@href').extract()[0]
                    exist = True
                    yield Request(url=response.urljoin(href), callback=self.parse_item)
                    break
            if not exist:
                logging.log(logging.WARNING, 'There has been no job in the page,url:{}'.format(response.url))
        page_bar = sel.css('div.pagerbar a:nth-last-child(3)')
        if page_bar:
            if page_bar.css('::text').extract()[0] == '下一页' and exist:
                yield Request(url=response.urljoin(page_bar.xpath('@href').extract()[0]), callback=self.parse)
            else:
                logging.log(logging.WARNING, 'something wrong with the next page,url:{}'.format(response.url))
        else:
            logging.log(logging.WARNING, 'something wrong with the next page,url:{}'.format(response.url))

    def parse_item(self, response):
        sel = Selector(response)
        item = JobItem()
        item['available_number'] = 'Unknown'
        item['job_category'] = 'Unknown'
        item['company_type'] = 'Unknown'
        item['url'] = response.url
        item['from_website'] = self.name
        try:
            item['name'] = sel.css('div.title-info>h1::text').extract()[0].strip()
        except IndexError:
            item['name'] = sel.css('div.job-title h1::text').extract()[0].strip()
        try:
            item['company_name'] = sel.css('div.title-info>h3 a::text').extract()[0].strip()
        except IndexError:
            try:
                item['company_name'] = sel.css('div.title-info>h3::text').extract()[0].strip()
            except IndexError:
                item['company_name'] = sel.css('div.job-title>h2::text').extract()[0].strip()
        item['salary'] = ''.join(sel.css('div.job-title-left>p.job-item-title::text').extract()).strip()
        if not item['salary']:
            item['salary'] = ''.join(sel.css('div.job-title-left>p.job-main-title::text').extract()).strip()
        try:
            location = sel.css('p.basic-infor>span>a::text').extract()[0].split('-')[0]
        except IndexError:
            location = ''.join(sel.css('p.basic-infor>span::text').extract()).strip()
        try:
            item['pub_date'] = sel.css('p.basic-infor time::text').extract()[0].strip()
        except IndexError:
            item['pub_date'] = sel.css('p.basic-infor span:last-child::text').extract()[1]
        try:
            item['edu'], item['exp'] = sel.css('div.job-qualifications span::text').extract()[:2]
        except ValueError:
            item['edu'], item['exp'] = sel.css('div.resume span::text').extract()[:2]
        com_info = sel.css('ul.new-compintro')
        try:
            item['company_industry'] = com_info.css('li:nth-child(1) a::text').extract()[0]
        except IndexError:
            try:
                item['company_industry'] = com_info.css('li:nth-child(1)::text').extract()[0].split('：')[1]
            except IndexError:
                item['company_industry'] = 'Unknown'
        try:
            scale = com_info.css('li:nth-child(2)::text').extract()[0][5:]
            if '号' not in scale:
                item['company_scale'] = scale
            else:
                item['address'] = scale
                item['company_scale'] = 'Unknown'
        except IndexError:
            item['company_scale'] = 'Unknown'
        if 'address' not in item:
            try:
                item['address'] = com_info.css('li:nth-child(3)::text').extract()[0][5:]
            except IndexError:
                item['address'] = 'Unknown'
        if location in self.city_list:
            item['location'] = location
        else:
            for city in self.city_list:
                if city in item['address']:
                    item['location'] = city
                    break
            else:
                logging.log(logging.WARNING, 'The city is not in the list,url:{}'.format(response.url))
                return
        text = ''.join(sel.css('div.content-word::text').extract()).strip()
        attribute = ['duty', 'demand', 'welfare', 'work_time', 'address']
        item['duty'] = item['demand'] = item['welfare'] = item['work_time'] = ''
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
        return item
