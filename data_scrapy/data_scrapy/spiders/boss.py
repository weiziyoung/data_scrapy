from data_scrapy.items import JobItem
from data_scrapy.base_spider import BaseSpider
from scrapy import Request
from scrapy.selector import Selector
from scrapy.exceptions import CloseSpider
import logging


class BossSpider(BaseSpider):
    name = 'boss'
    allowed_domains = ['zhipin.com']
    custom_settings = {
        'DOWNLOAD_DELAY': 3
    }

    def __init__(self):
        super(BossSpider, self).__init__()

    def start_requests(self):
        start_urls = []
        for word in self.INSPECT_WORDS:
            start_urls.append('https://www.zhipin.com/c100010000/h_101190400/?query={}&page=1&ka=page-1'.format(word))
        for url in start_urls:
            yield Request(url=url, callback=self.parse)

    def parse(self, response):
        if response.status != 200:
            raise CloseSpider(reason='This spider has been blocked')
        count = 3
        sel = Selector(response)
        results = sel.css('div.job-list ul li')
        for result in results:
            temp = result.css('div.job-primary')
            job_name, company_name = temp.css('div.info-primary h3.name a div.job-title::text').extract()[0],\
                                     temp.css('div.info-company div.company-text h3.name a::text').extract()[0]
            flag = False
            for word in self.FILTER_WORDS:
                if word in job_name or word in company_name:
                    href = temp.css('div.info-primary h3.name a').xpath('@href').extract()[0]
                    count = 3
                    flag = True
                    yield Request(response.urljoin(href), callback=self.parse_item)
                    break
            if not flag:
                logging.log(logging.INFO, "There is no key word in the position's name--{}".format(job_name))
                count -= 1
                if not count:
                    logging.log(logging.INFO, "No key words for three times,Stop!")
                return
        next_page = sel.css("[ka='page-next']")
        if next_page:
            next_page = next_page.xpath('@href').extract()[0]
            yield Request(url=response.urljoin(next_page), callback=self.parse)

    def parse_item(self, response):
        if response.status != 200:
            raise CloseSpider(reason='This spider has been blocked')
        sel = Selector(response)
        item = JobItem()
        item['url'] = response.url
        item['from_website'] = self.name
        item['job_category'] = 'Unknown'
        item['available_number'] = 'Unknown'
        info_primary = sel.css('div.job-banner div.info-primary')
        item['pub_date'] = info_primary.css('span.time::text').extract()[0]
        item['name'] = info_primary.css('div.name h1::text').extract()[0]
        item['salary'] = info_primary.css('span.badge::text').extract()[0]
        info_set = info_primary.css('p::text').extract()
        item['location'], item['exp'], item['edu'] = [each.split('：')[1] for each in info_set]
        info_company = sel.css('div.info-company')
        item['company_name'] = info_company.css('h3.name a::text').extract()[0]
        info_detail = info_company.css('p::text')
        try:
            item['company_scale'] = info_detail.extract()[1]
        except IndexError:
            item['company_scale'] = info_detail.extract()[0]
        item['company_industry'] = info_company.css('p>a::text').extract()[0]
        # In fact,there is no information about the type of the company in this website
        item['company_type'] = 'Unknown'
        result = sel.css('div.job-sec>div.text::text').extract()
        text = ''.join([each.strip()for each in result])
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
        item['address'] = sel.css('div.location-address::text').extract()[0]
        yield item
