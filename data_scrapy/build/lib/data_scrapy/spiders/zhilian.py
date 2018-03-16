from data_scrapy.items import JobItem
from data_scrapy.base_spider import BaseSpider
from scrapy.selector import Selector
from scrapy import Request
import logging
import re


class ZhilianSpider(BaseSpider):
    name = 'zhilian'
    allowed_domains = ['sou.zhaopin.com', 'jobs.zhaopin.com']

    def start_requests(self):
        start_urls = []
        for city in self.city_list:
            for word in self.INSPECT_WORDS:
                start_urls.append('http://sou.zhaopin.com/jobs/searchresult.ashx?jl={}&kw={}&p=1'.format(city, word))
        for url in start_urls:
            yield Request(url=url, callback=self.parse)

    def parse(self, response):
        sel = Selector(response)
        results = sel.xpath('//table[@class="newlist" and position()>1]')
        count = 3
        for result in results:
            job_name = result.xpath('tr/td[1]/div/a').xpath('string(.)').extract()[0]
            company_name = result.xpath('tr[1]/td[3]/a[1]').xpath('string(.)').extract()[0]
            flag = False
            for word in self.FILTER_WORDS:
                if word in job_name or word in company_name:
                    url = result.xpath('tr/td[1]/div/a/@href').extract()[0]
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
        next_page = sel.xpath('//*[@class="pagesDown-pos"]/a/@href')
        if next_page:
            yield Request(url=next_page.extract()[0], callback=self.parse)

    def parse_item(self, response):
        if response.selector.xpath('//*[@class="outmoded_container_img"]'):
            logging.log(logging.INFO, 'Invalid job position')
            return
        if not response.selector.xpath('//div[@class="terminalpage-left"]'):
            logging.log(logging.INFO, 'unconventional page'+response.url)
            return
        item = JobItem()
        try:
            item['company_scale'] = \
                response.selector.xpath('/html/body/div[6]/div[2]/div[1]/ul/li[1]/strong/text()').extract()[0]
        except IndexError:
            logging.log(logging.INFO, 'No company info'+response.url)
            return
        item['company_type'] = \
            response.selector.xpath('/html/body/div[6]/div[2]/div[1]/ul/li[2]/strong/text()').extract()[0]
        item['company_industry'] = \
            response.selector.xpath('/html/body/div[6]/div[2]/div[1]/ul/li[3]/strong//text()').extract()[0]
        attribute = ['duty', 'demand', 'welfare', 'work_time', 'address']
        item['name'] = response.selector.xpath('/html/body/div[5]/div[1]/div[1]/h1/text()').extract()[0]
        item['location'] = response.selector.xpath('/html/body/div[6]/div[1]/ul/li[2]/strong/a/text()').extract()[0]
        if item['location'] not in self.city_list:
            logging.log(logging.WARNING, 'The city is not in the list')
            return
        item['salary'] = response.selector.xpath('/html/body/div[6]/div[1]/ul/li[1]/strong/text()').extract()[0].strip()
        item['pub_date'] = response.selector.xpath('//*[@id="span4freshdate"]/text()').extract()[0]
        item['job_category'] = response.selector.xpath('/html/body/div[6]/div[1]/ul/li[4]/strong/text()').extract()[0]
        item['exp'] = response.selector.xpath('/html/body/div[6]/div[1]/ul/li[5]/strong/text()').extract()[0]
        item['available_number'] = response.selector.xpath('/html/body/div[6]/div[1]/ul/li[7]/strong/text()').extract()[0]
        item['edu'] = response.selector.xpath('/html/body/div[6]/div[1]/ul/li[6]/strong/text()').extract()[0]
        item['duty'] = item['demand'] = item['welfare'] = item['work_time'] = item['address'] = ''
        item['company_name'] = response.selector.xpath('/html/body/div[6]/div[2]/div[1]/p/a/text()').extract()[0]
        text = response.selector.xpath('//*[@class="tab-cont-box"]/div[1]')[0].xpath('string(.)').extract()[0]
        text = re.sub(r'\s|[|]', '', text)
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
            if flag == len(flags)-1:
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
        item['from_website'] = self.name
        item['url'] = response.url
        yield item



