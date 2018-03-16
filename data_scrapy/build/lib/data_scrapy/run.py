from spiders.boss import BossSpider
from spiders.job51 import Job51Spider
from spiders.liepin import LiePinSpider
from spiders.zhilian import ZhilianSpider
import scrapy
from scrapy.crawler import CrawlerProcess
process = CrawlerProcess({
  'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'
})
process.crawl(Job51Spider)
process.crawl(LiePinSpider)
process.crawl(ZhilianSpider)
process.crawl(BossSpider)
process.start()